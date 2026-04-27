from flask import Blueprint, jsonify, request
import traceback
from sqlalchemy import text

# 1. Create a new Blueprint
lists_bp = Blueprint('lists_api', __name__)

def get_adjustment_fields_sql():
    return """
    (GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF(coeff, 0), 1) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF(coeff, 0), 1) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS season_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS event_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / NULLIF(age_ratio_male, 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / NULLIF(age_ratio_male, 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS age_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / NULLIF(age_ratio_sex, 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / NULLIF(age_ratio_sex, 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS age_sex_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_male, 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_male, 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS age_event_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_sex, 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_sex, 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS age_sex_event_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) * (age_ratio_sex / NULLIF(age_ratio_male, 0)) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) * (age_ratio_sex / NULLIF(age_ratio_male, 0)) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS sex_event_adj_time,
    (GREATEST(COALESCE(CAST(time_seconds / NULLIF((age_ratio_sex / NULLIF(age_ratio_male, 0)), 0) AS INTEGER), 0), :min_sec) / 60)::text || ':' || lpad((GREATEST(COALESCE(CAST(time_seconds / NULLIF((age_ratio_sex / NULLIF(age_ratio_male, 0)), 0) AS INTEGER), 0), :min_sec) % 60)::text, 2, '0') AS sex_adj_time,
    time_seconds,

    GREATEST(COALESCE(time_seconds / COALESCE(NULLIF(coeff, 0), 1), 0), :min_sec) AS season_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1), 0), :min_sec) AS event_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / NULLIF(age_ratio_male, 0), 0), :min_sec) AS age_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / NULLIF(age_ratio_sex, 0), 0), :min_sec) AS age_sex_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_male, 0), 0), :min_sec) AS age_event_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) / NULLIF(age_ratio_sex, 0), 0), :min_sec) AS age_sex_event_adj_time_seconds,
    GREATEST(COALESCE(time_seconds / COALESCE(NULLIF((COALESCE(NULLIF(coeff, 0), 1) + COALESCE(NULLIF(coeff_event, 0), 1) - 1), 0), 1) * (age_ratio_sex / NULLIF(age_ratio_male, 0)), 0), :min_sec) AS sex_event_adj_time_seconds,
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
    from app import db  # Import db here to avoid circular import
    from flask import request
    try:
        # Sort whitelist mapped to pre-built materialized views.
        sort_to_view_all_time = {
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
        
        sort_to_view_last_year = {
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
        
        sort_to_view = sort_to_view_last_year if period == 'last_year' else sort_to_view_all_time
        
        sort = request.args.get('sort', 'time_seconds')
        if sort not in sort_to_view:
            return jsonify({
                'error': 'Invalid sort column',
                'allowed': sorted(list(sort_to_view.keys()))
            }), 400
            
        direction = request.args.get('direction', 'asc').lower()
        try:
            limit = int(request.args.get('limit', 1000))
        except (ValueError, TypeError):
            limit = 1000

        # Validate inputs
        if sort not in sort_to_view:
            return jsonify({
                'error': 'Invalid sort column',
                'allowed': sorted(list(sort_to_view.keys()))
            }), 400

        if direction not in ('asc', 'desc'):
            direction = 'asc'

        # Cap limit to a reasonable maximum
        if limit < 1:
            limit = 1
        elif limit > 10000:
            limit = 10000

        selected_view = sort_to_view[sort]

        # Build and execute query from the selected materialized view.
        sql = f"""
            SELECT *
            FROM {selected_view}
            ORDER BY {sort} {direction.upper()}, athlete_code
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
        from app import db

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

                sql = text("""
                        WITH base AS (
                            SELECT
                                athlete_code,
                                name,
                                club,
                                time_seconds,
                                event_adj_time_seconds,
                                age_ratio_male,
                                age_ratio_sex,
                                best_curve_ranking_current,
                                best_curve_ranking_historic,
                                best_curve_ranking_current_type,
                                event_dt
                            FROM mv_extend_runs
                            WHERE event_code = :event_code
                                AND athlete_code IS NOT NULL
                                AND event_dt IS NOT NULL
                        ),
                        agg AS (
                            SELECT
                                athlete_code,
                                MIN(time_seconds) AS min_time_seconds,
                                MIN(event_adj_time_seconds) AS min_event_adj_time_seconds,
                                MIN(event_adj_time_seconds / NULLIF(age_ratio_male, 0)) AS min_age_event_adj_time_seconds,
                                MIN(event_adj_time_seconds / NULLIF(age_ratio_sex, 0)) AS min_age_sex_event_adj_time_seconds,
                                COUNT(*) AS appearances
                            FROM base
                            GROUP BY athlete_code
                        ),
                        latest AS (
                            SELECT DISTINCT ON (athlete_code)
                                athlete_code,
                                name,
                                club,
                                best_curve_ranking_current,
                                best_curve_ranking_historic,
                                best_curve_ranking_current_type,
                                event_dt AS last_run_date
                            FROM base
                            ORDER BY athlete_code, event_dt DESC
                        ),
                        latest_any_event AS (
                            SELECT
                                athlete_code,
                                last_any_run_date
                            FROM mv_athlete_last_any_run
                        ),
                        vol_base AS (
                            SELECT
                                v.athlete_code,
                                CASE
                                    WHEN v.event_date ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN to_date(v.event_date, 'DD/MM/YYYY')
                                    WHEN v.event_date ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN to_date(v.event_date, 'YYYY-MM-DD')
                                    ELSE NULL
                                END AS vol_dt
                            FROM volunteers v
                            WHERE v.event_code = :event_code
                                AND v.athlete_code IS NOT NULL
                        ),
                        vol_counts AS (
                            SELECT
                                athlete_code,
                                COUNT(*) AS volunteer_count,
                                MAX(vol_dt) AS last_volunteer_date
                            FROM vol_base
                            GROUP BY athlete_code
                        )
                        SELECT
                            a.athlete_code,
                            l.name,
                            l.club,
                            to_char((a.min_time_seconds::int || ' seconds')::interval, 'FMMI:SS') AS min_time_mmss,
                            to_char((round(a.min_event_adj_time_seconds)::int || ' seconds')::interval, 'FMMI:SS') AS min_event_adj_mmss,
                            to_char((round(a.min_age_event_adj_time_seconds)::int || ' seconds')::interval, 'FMMI:SS') AS min_age_event_adj_mmss,
                            to_char((round(a.min_age_sex_event_adj_time_seconds)::int || ' seconds')::interval, 'FMMI:SS') AS min_age_sex_event_adj_mmss,
                            a.appearances,
                            COALESCE(v.volunteer_count, 0) AS volunteer_count,
                            (a.appearances + COALESCE(v.volunteer_count, 0)) AS total_count,
                            CASE
                                WHEN la.last_any_run_date < (current_date - INTERVAL '1 year') THEN NULL
                                ELSE l.best_curve_ranking_current
                            END AS best_curve_ranking_current,
                            l.best_curve_ranking_historic,
                            l.best_curve_ranking_current_type,
                            to_char(l.last_run_date, 'DD/MM/YYYY') AS last_run_date,
                            (current_date - l.last_run_date) AS days_since_last_run,
                            to_char(v.last_volunteer_date, 'DD/MM/YYYY') AS last_volunteer_date,
                            CASE
                                WHEN v.last_volunteer_date IS NULL THEN NULL
                                ELSE (current_date - v.last_volunteer_date)
                            END AS days_since_last_volunteered
                        FROM agg a
                        JOIN latest l USING (athlete_code)
                        LEFT JOIN latest_any_event la USING (athlete_code)
                        LEFT JOIN vol_counts v USING (athlete_code)
                        ORDER BY total_count DESC, a.appearances DESC, volunteer_count DESC
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

