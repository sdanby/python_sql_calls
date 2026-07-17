from flask import Blueprint, current_app, jsonify, request
import traceback
from sqlalchemy import text

# 1. Create a new Blueprint
lists_bp = Blueprint('lists_api', __name__)


def _get_db():
    db = current_app.extensions.get('sqlalchemy')
    if db is None:
        raise RuntimeError('SQLAlchemy extension is not registered on the active Flask app.')
    return db

def get_adjustment_fields_sql():
    return """
    (GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF(coeff, 0), 1) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF(coeff, 0), 1) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS season_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS event_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / NULLIF(age_ratio_male, 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / NULLIF(age_ratio_male, 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS age_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / NULLIF(age_ratio_sex, 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / NULLIF(age_ratio_sex, 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS age_sex_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_male, 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_male, 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS age_event_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_sex, 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_sex, 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS age_sex_event_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / (COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) * (age_ratio_sex / NULLIF(age_ratio_male, 0))) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / (COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) * (age_ratio_sex / NULLIF(age_ratio_male, 0))) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS sex_event_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / NULLIF((age_ratio_sex / NULLIF(age_ratio_male, 0)), 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / NULLIF((age_ratio_sex / NULLIF(age_ratio_male, 0)), 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS sex_adj_time,
    time_seconds,

    GREATEST(COALESCE(time_seconds / COALESCE(NULLIF(coeff, 0), 1), 0), :min_sec) AS season_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1), 0), :min_sec) AS event_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / NULLIF(age_ratio_male, 0), 0), :min_sec) AS age_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / NULLIF(age_ratio_sex, 0), 0), :min_sec) AS age_sex_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_male, 0), 0), :min_sec) AS age_event_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_sex, 0), 0), :min_sec) AS age_sex_event_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / (COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) * (age_ratio_sex / NULLIF(age_ratio_male, 0))), 0), :min_sec) AS sex_event_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / (age_ratio_sex / NULLIF(age_ratio_male, 0)), 0), :min_sec) AS sex_adj_time_seconds
    """
    #(GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF(coeff, 0), 1) / NULLIF(age_ratio_male, 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF(coeff, 0), 1) / NULLIF(age_ratio_male, 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS age_season_adj_time,
    #(GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF(coeff, 0), 1) / NULLIF(age_ratio_sex, 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF(coeff, 0), 1) / NULLIF(age_ratio_sex, 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS age_sex_season_adj_time,
   

# 2. Define the new API endpoint for the fastest runs list
@lists_bp.route('/api/lists/fastest_runs', methods=['GET'])
def get_fastest_runs_by_athlete():
    """
    API endpoint to get the single fastest run for every athlete.
    """
    db = _get_db()
    try:
        # Representative-row whitelist mapped to pre-built materialized views.
        view_sort_to_view_all_time = {
            'time_seconds': 'mv_best_curve',
            'season_adj_time_seconds': 'mv_best_season_curve',
            'event_adj_time_seconds': 'mv_best_event_curve',
            'age_adj_time_seconds': 'mv_best_age_curve',
            'sex_adj_time_seconds': 'mv_best_sex_curve',
            'age_sex_adj_time_seconds': 'mv_best_age_sex_curve',
            'age_event_adj_time_seconds': 'mv_best_age_event_curve',
            'sex_event_adj_time_seconds': 'mv_best_sex_event_curve',
            'age_sex_event_adj_time_seconds': 'mv_best_age_sex_event_curve',
        }
        
        view_sort_to_view_last_year = {
            'time_seconds': 'mv_best_1y_curve',
            'season_adj_time_seconds': 'mv_best_season_1y_curve',
            'event_adj_time_seconds': 'mv_best_event_1y_curve',
            'age_adj_time_seconds': 'mv_best_age_1y_curve',
            'sex_adj_time_seconds': 'mv_best_sex_1y_curve',
            'age_sex_adj_time_seconds': 'mv_best_age_sex_1y_curve',
            'age_event_adj_time_seconds': 'mv_best_age_event_1y_curve',
            'sex_event_adj_time_seconds': 'mv_best_sex_event_1y_curve',
            'age_sex_event_adj_time_seconds': 'mv_best_age_sex_event_1y_curve',
        }

        period = request.args.get('period', 'all_time').lower()
        if period not in ('all_time', 'last_year'):
            return jsonify({
                'error': 'Invalid period',
                'allowed': ['all_time', 'last_year']
            }), 400

        participant_filter_aliases = {
            'all': 'all_participants',
            'all_participants': 'all_participants',
            'gt_50_total_runs': 'gt_50_total_runs',
            'gt_50_local_runs': 'gt_50_local_runs',
            'gt_10_local_runs_1y': 'gt_10_local_runs_1y',
        }
        participant_filter = participant_filter_aliases.get(
            request.args.get('participant_filter', 'all_participants').lower(),
        )
        if participant_filter is None:
            return jsonify({
                'error': 'Invalid participant filter',
                'allowed': sorted(list(participant_filter_aliases.keys()))
            }), 400

        participant_filter_sql = {
            'all_participants': '1=1',
            'gt_50_total_runs': 'COALESCE(f.total_runs_all_parkruns, 0) > 50',
            'gt_50_local_runs': 'COALESCE(f.total_runs_local_parkruns, 0) > 50',
            'gt_10_local_runs_1y': 'COALESCE(f.total_runs_local_parkruns_1y, 0) > 10',
        }

        view_sort_to_view = view_sort_to_view_last_year if period == 'last_year' else view_sort_to_view_all_time
        current_rank_sql_by_view = {
            'time_seconds': 'latest.current_best_rank_b',
            'season_adj_time_seconds': 'latest.best_curve_ranking_current',
            'event_adj_time_seconds': 'latest.current_best_rank_e',
            'age_adj_time_seconds': 'latest.best_curve_ranking_current',
            'sex_adj_time_seconds': 'latest.best_curve_ranking_current',
            'age_sex_adj_time_seconds': 'latest.best_curve_ranking_current',
            'age_event_adj_time_seconds': 'latest.current_best_rank_ae',
            'sex_event_adj_time_seconds': 'latest.current_best_rank_es',
            'age_sex_event_adj_time_seconds': 'latest.current_best_rank_aes',
        }
        current_rank_sql_by_view_last_year = {
            'time_seconds': 'COALESCE(v.period_best_rank, latest.current_best_rank_b)',
            'season_adj_time_seconds': 'latest.best_curve_ranking_current',
            'event_adj_time_seconds': 'COALESCE(v.period_best_rank, latest.current_best_rank_e)',
            'age_adj_time_seconds': 'latest.best_curve_ranking_current',
            'sex_adj_time_seconds': 'latest.best_curve_ranking_current',
            'age_sex_adj_time_seconds': 'latest.best_curve_ranking_current',
            'age_event_adj_time_seconds': 'COALESCE(v.period_best_rank, latest.current_best_rank_ae)',
            'sex_event_adj_time_seconds': 'COALESCE(v.period_best_rank, latest.current_best_rank_es)',
            'age_sex_event_adj_time_seconds': 'COALESCE(v.period_best_rank, latest.current_best_rank_aes)',
        }
        order_by_sql = {
            'time_seconds': 'v.time_seconds',
            'season_adj_time_seconds': 'v.season_adj_time_seconds',
            'event_adj_time_seconds': 'v.event_adj_time_seconds',
            'age_adj_time_seconds': 'v.age_adj_time_seconds',
            'sex_adj_time_seconds': 'v.sex_adj_time_seconds',
            'age_sex_adj_time_seconds': 'v.age_sex_adj_time_seconds',
            'age_event_adj_time_seconds': 'v.age_event_adj_time_seconds',
            'sex_event_adj_time_seconds': 'v.sex_event_adj_time_seconds',
            'age_sex_event_adj_time_seconds': 'v.age_sex_event_adj_time_seconds',
            'total_runs_all_parkruns': 'COALESCE(f.total_runs_all_parkruns, 0)',
            'total_runs_local_parkruns': 'COALESCE(f.total_runs_local_parkruns, 0)',
            'total_runs_local_parkruns_1y': 'COALESCE(f.total_runs_local_parkruns_1y, 0)',
        }
        view_order_by_sql = {
            'time_seconds': 'v0.time_seconds',
            'season_adj_time_seconds': 'v0.season_adj_time_seconds',
            'event_adj_time_seconds': 'v0.event_adj_time_seconds',
            'age_adj_time_seconds': 'v0.age_adj_time_seconds',
            'sex_adj_time_seconds': 'v0.sex_adj_time_seconds',
            'age_sex_adj_time_seconds': 'v0.age_sex_adj_time_seconds',
            'age_event_adj_time_seconds': 'v0.age_event_adj_time_seconds',
            'sex_event_adj_time_seconds': 'v0.sex_event_adj_time_seconds',
            'age_sex_event_adj_time_seconds': 'v0.age_sex_event_adj_time_seconds',
        }

        view_sort = request.args.get('view_sort', request.args.get('sort', 'time_seconds'))
        if view_sort not in view_sort_to_view:
            return jsonify({
                'error': 'Invalid representative view column',
                'allowed': sorted(list(view_sort_to_view.keys()))
            }), 400

        sort = request.args.get('sort', 'time_seconds')
        if sort not in order_by_sql:
            return jsonify({
                'error': 'Invalid sort column',
                'allowed': sorted(list(order_by_sql.keys()))
            }), 400
            
        direction = request.args.get('direction', 'asc').lower()
        try:
            limit = int(request.args.get('limit', 1000))
        except (ValueError, TypeError):
            limit = 1000

        if direction not in ('asc', 'desc'):
            direction = 'asc'

        # Cap limit to a reasonable maximum
        if limit < 1:
            limit = 1
        elif limit > 10000:
            limit = 10000

        selected_view = view_sort_to_view[view_sort]
        selected_historic_view = view_sort_to_view_all_time[view_sort]
        selected_current_rank_sql = (current_rank_sql_by_view_last_year if period == 'last_year' else current_rank_sql_by_view).get(view_sort, 'NULL')
        selected_historic_rank_sql = 'COALESCE(hist.rank, v.rank)' if period == 'last_year' else 'v.rank'
        selected_participant_filter = participant_filter_sql[participant_filter]
        selected_order_by = order_by_sql[sort]
        selected_view_order_by = view_order_by_sql[view_sort]
        selection_scope = request.args.get('selection_scope', 'all_eligible').lower()
        if selection_scope not in ('all_eligible', 'selected_view_top_1000'):
            return jsonify({
                'error': 'Invalid selection scope',
                'allowed': ['all_eligible', 'selected_view_top_1000']
            }), 400

        # Build and execute query from the selected materialized view.
        if selection_scope == 'selected_view_top_1000':
            sql = f"""
                WITH eligible_athletes AS (
                    SELECT v0.athlete_code
                    FROM {selected_view} v0
                    LEFT JOIN mv_participant_run_filters f0
                      ON f0.athlete_code = v0.athlete_code
                    WHERE {selected_participant_filter.replace('f.', 'f0.')}
                    ORDER BY {selected_view_order_by} ASC, v0.athlete_code
                    LIMIT :limit
                ), latest_rank AS (
                    SELECT
                        m.athlete_code,
                        m.current_best_rank_b,
                        m.best_curve_ranking_current,
                        m.current_best_rank_e,
                        m.current_best_rank_ae,
                        m.current_best_rank_es,
                        m.current_best_rank_aes
                    FROM mv_latest_curve_ranks m
                )
                SELECT
                    v.*,
                                        CAST({selected_historic_rank_sql} AS numeric) AS ev_rank,
                    CAST({selected_current_rank_sql} AS numeric) AS cur_rank,
                    f.total_runs_all_parkruns,
                    f.total_runs_local_parkruns,
                    f.total_runs_local_parkruns_1y
                FROM eligible_athletes ea
                JOIN {selected_view} v
                  ON v.athlete_code = ea.athlete_code
                                LEFT JOIN {selected_historic_view} hist
                                    ON hist.athlete_code = v.athlete_code
                LEFT JOIN latest_rank latest
                  ON latest.athlete_code = v.athlete_code
                LEFT JOIN mv_participant_run_filters f
                  ON f.athlete_code = v.athlete_code
                ORDER BY {selected_order_by} {direction.upper()}, v.athlete_code;
            """
        else:
            sql = f"""
                WITH latest_rank AS (
                    SELECT
                        m.athlete_code,
                        m.current_best_rank_b,
                        m.best_curve_ranking_current,
                        m.current_best_rank_e,
                        m.current_best_rank_ae,
                        m.current_best_rank_es,
                        m.current_best_rank_aes
                    FROM mv_latest_curve_ranks m
                )
                SELECT
                    v.*, 
                                        CAST({selected_historic_rank_sql} AS numeric) AS ev_rank,
                    CAST({selected_current_rank_sql} AS numeric) AS cur_rank,
                    f.total_runs_all_parkruns,
                    f.total_runs_local_parkruns,
                    f.total_runs_local_parkruns_1y
                FROM {selected_view} v
                                LEFT JOIN {selected_historic_view} hist
                                    ON hist.athlete_code = v.athlete_code
                LEFT JOIN latest_rank latest
                  ON latest.athlete_code = v.athlete_code
                LEFT JOIN mv_participant_run_filters f
                  ON f.athlete_code = v.athlete_code
                WHERE {selected_participant_filter}
                ORDER BY {selected_order_by} {direction.upper()}, v.athlete_code
                LIMIT :limit;
            """

        sql_query = text(sql)
        result_proxy = db.session.execute(sql_query, {'limit': limit})

        # Fetch column names from the result proxy
        column_names = result_proxy.keys()

        # Create a list of dictionaries for the JSON response
        results = [dict(zip(column_names, row)) for row in result_proxy.fetchall()]

        # Commit (select-only, but keep parity with remote implementation)
        db.session.commit()

        return jsonify(results)

    except Exception as e:
        # Rollback any transactional state and log the full traceback for debugging
        try:
            db.session.rollback()
        except Exception:
            pass
        tb = traceback.format_exc()
        print(f"Database error in get_fastest_runs_by_athlete: {e}\n{tb}")
        # Return error details to help debugging (consider removing in production)
        return jsonify({
            "error": "Failed to fetch data from the database",
            "exception": str(e),
            "traceback": tb
        }), 500


@lists_bp.route('/api/lists/event_summary', methods=['GET'])
def get_event_summary_by_code():
    """Return event summary leaderboard for a specific event_code."""
    db = _get_db()

    def relation_exists(relation_name):
        exists_sql = text("SELECT to_regclass(:relation_name) IS NOT NULL")
        return bool(db.session.execute(exists_sql, {'relation_name': relation_name}).scalar())

    try:
        event_code_raw = request.args.get('event_code', type=str)
        if event_code_raw is None:
            return jsonify({"error": "event_code is required"}), 400

        try:
            event_code = int(event_code_raw)
        except (TypeError, ValueError):
            return jsonify({"error": "event_code must be an integer"}), 400

        if event_code < 1:
            return jsonify({"error": "event_code must be >= 1"}), 400

        try:
            limit = int(request.args.get('limit', 250))
        except (TypeError, ValueError):
            limit = 250
        if limit < 1:
            limit = 1
        elif limit > 1000:
            limit = 1000

        if relation_exists('public.mv_event_summary_cache'):
            sql = text("""
                SELECT
                    summary.athlete_code,
                    summary.name,
                    age_lookup.age_group,
                    summary.club,
                    summary.min_time_mmss,
                    summary.min_event_adj_mmss,
                    summary.min_age_event_adj_mmss,
                    summary.min_age_sex_event_adj_mmss,
                    summary.appearances,
                    summary.volunteer_count,
                    summary.total_count,
                    summary.best_curve_ranking_current,
                    summary.best_curve_ranking_historic,
                    summary.best_curve_ranking_current_type,
                    summary.last_run_date_ddmmyyyy AS last_run_date,
                    summary.days_since_last_run,
                    summary.last_volunteer_date_ddmmyyyy AS last_volunteer_date,
                    summary.days_since_last_volunteered
                FROM mv_event_summary_cache summary
                LEFT JOIN LATERAL (
                    SELECT er.age_group
                    FROM mv_extend_runs er
                    WHERE er.event_code = summary.event_code
                      AND er.athlete_code = summary.athlete_code
                      AND er.event_dt IS NOT NULL
                    ORDER BY er.event_dt DESC
                    LIMIT 1
                ) age_lookup ON TRUE
                WHERE summary.event_code = :event_code
                ORDER BY summary.total_count DESC, summary.appearances DESC, summary.volunteer_count DESC, summary.athlete_code
                LIMIT :limit;
            """)
        else:
            sql = text("""
                WITH base AS (
                    SELECT
                        er.event_code,
                        er.athlete_code,
                        er.name,
                        er.club,
                        er.age_group,
                        er.time_seconds,
                        er.event_adj_time_seconds,
                        er.age_ratio_male,
                        er.age_ratio_sex,
                        er.best_curve_ranking_current,
                        er.best_curve_ranking_historic,
                        er.best_curve_ranking_current_type,
                        er.event_dt
                    FROM mv_extend_runs er
                    WHERE er.event_code = :event_code
                      AND er.athlete_code IS NOT NULL
                      AND er.event_dt IS NOT NULL
                ),
                agg AS (
                    SELECT
                        event_code,
                        athlete_code,
                        MIN(time_seconds) AS min_time_seconds,
                        MIN(event_adj_time_seconds) AS min_event_adj_time_seconds,
                        MIN(event_adj_time_seconds / NULLIF(age_ratio_male, 0)) AS min_age_event_adj_time_seconds,
                        MIN(event_adj_time_seconds / NULLIF(age_ratio_sex, 0)) AS min_age_sex_event_adj_time_seconds,
                        COUNT(*) AS appearances
                    FROM base
                    GROUP BY event_code, athlete_code
                ),
                latest_event AS (
                    SELECT DISTINCT ON (event_code, athlete_code)
                        event_code,
                        athlete_code,
                        name,
                        club,
                        age_group,
                        best_curve_ranking_current,
                        best_curve_ranking_historic,
                        best_curve_ranking_current_type,
                        event_dt AS last_run_date
                    FROM base
                    ORDER BY event_code, athlete_code, event_dt DESC
                ),
                latest_any_event AS (
                    SELECT
                        er.athlete_code,
                        MAX(er.event_dt) AS last_any_run_date
                    FROM mv_extend_runs er
                    WHERE er.athlete_code IS NOT NULL
                      AND er.event_dt IS NOT NULL
                    GROUP BY er.athlete_code
                ),
                vol_base AS (
                    SELECT
                        v.event_code,
                        v.athlete_code,
                        CASE
                            WHEN v.event_date ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(v.event_date, 'DD/MM/YYYY')
                            WHEN v.event_date ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(v.event_date, 'YYYY-MM-DD')
                            ELSE NULL
                        END AS vol_dt
                    FROM volunteers v
                    WHERE v.event_code = :event_code
                      AND v.athlete_code IS NOT NULL
                ),
                vol_counts AS (
                    SELECT
                        event_code,
                        athlete_code,
                        COUNT(*) AS volunteer_count,
                        MAX(vol_dt) AS last_volunteer_date
                    FROM vol_base
                    GROUP BY event_code, athlete_code
                )
                SELECT
                    agg.athlete_code,
                    latest_event.name,
                    latest_event.age_group,
                    latest_event.club,
                    to_char((agg.min_time_seconds::int || ' seconds')::interval, 'FMMI:SS') AS min_time_mmss,
                    to_char((round(agg.min_event_adj_time_seconds)::int || ' seconds')::interval, 'FMMI:SS') AS min_event_adj_mmss,
                    to_char((round(agg.min_age_event_adj_time_seconds)::int || ' seconds')::interval, 'FMMI:SS') AS min_age_event_adj_mmss,
                    to_char((round(agg.min_age_sex_event_adj_time_seconds)::int || ' seconds')::interval, 'FMMI:SS') AS min_age_sex_event_adj_mmss,
                    agg.appearances,
                    COALESCE(vol_counts.volunteer_count, 0) AS volunteer_count,
                    agg.appearances + COALESCE(vol_counts.volunteer_count, 0) AS total_count,
                    CASE
                        WHEN latest_any_event.last_any_run_date < (current_date - INTERVAL '1 year') THEN NULL
                        ELSE latest_event.best_curve_ranking_current
                    END AS best_curve_ranking_current,
                    latest_event.best_curve_ranking_historic,
                    latest_event.best_curve_ranking_current_type,
                    to_char(latest_event.last_run_date, 'DD/MM/YYYY') AS last_run_date,
                    (current_date - latest_event.last_run_date) AS days_since_last_run,
                    to_char(vol_counts.last_volunteer_date, 'DD/MM/YYYY') AS last_volunteer_date,
                    CASE
                        WHEN vol_counts.last_volunteer_date IS NULL THEN NULL
                        ELSE (current_date - vol_counts.last_volunteer_date)
                    END AS days_since_last_volunteered
                FROM agg
                JOIN latest_event
                  ON latest_event.event_code = agg.event_code
                 AND latest_event.athlete_code = agg.athlete_code
                LEFT JOIN latest_any_event
                  ON latest_any_event.athlete_code = agg.athlete_code
                LEFT JOIN vol_counts
                  ON vol_counts.event_code = agg.event_code
                 AND vol_counts.athlete_code = agg.athlete_code
                ORDER BY total_count DESC, agg.appearances DESC, volunteer_count DESC, agg.athlete_code
                LIMIT :limit;
            """)

        result_proxy = db.session.execute(sql, {'event_code': event_code, 'limit': limit})
        column_names = result_proxy.keys()
        results = [dict(zip(column_names, row)) for row in result_proxy.fetchall()]
        db.session.commit()
        return jsonify(results)

    except Exception as e:
        try:
            db.session.rollback()
        except Exception:
            pass
        tb = traceback.format_exc()
        print(f"Database error in get_event_summary_by_code: {e}\n{tb}")
        return jsonify({
            "error": "Failed to fetch data from the database",
            "exception": str(e),
            "traceback": tb
        }), 500

