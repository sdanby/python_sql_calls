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
    from app import db  # Import db here to avoid circular import
    from flask import request
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
        selected_participant_filter = participant_filter_sql[participant_filter]
        selected_order_by = order_by_sql[sort]

        # Build and execute query from the selected materialized view.
        sql = f"""
            SELECT
                v.*, 
                f.total_runs_all_parkruns,
                f.total_runs_local_parkruns,
                f.total_runs_local_parkruns_1y
            FROM {selected_view} v
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
                        SELECT
                            athlete_code,
                            name,
                            club,
                            min_time_mmss,
                            min_event_adj_mmss,
                            min_age_event_adj_mmss,
                            min_age_sex_event_adj_mmss,
                            appearances,
                            volunteer_count,
                            total_count,
                            best_curve_ranking_current,
                            best_curve_ranking_historic,
                            best_curve_ranking_current_type,
                            last_run_date_ddmmyyyy AS last_run_date,
                            days_since_last_run,
                            last_volunteer_date_ddmmyyyy AS last_volunteer_date,
                            days_since_last_volunteered
                        FROM mv_event_summary_cache
                        WHERE event_code = :event_code
                        ORDER BY total_count DESC, appearances DESC, volunteer_count DESC, athlete_code
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

