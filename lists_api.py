from flask import Blueprint, jsonify
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
        sort_to_view = {
            'time_seconds': 'mv_best_time',
            'season_adj_time_seconds': 'mv_best_season',
            'event_adj_time_seconds': 'mv_best_event',
            'age_adj_time_seconds': 'mv_best_age',
            'sex_adj_time_seconds': 'mv_best_sex',
            'age_sex_adj_time_seconds': 'mv_best_age_sex',
            'age_event_adj_time_seconds': 'mv_best_age_event',
            'sex_event_adj_time_seconds': 'mv_best_sex_event',
            'age_sex_event_adj_time_seconds': 'mv_best_age_sex_event'
        }

        # Read query params
        sort = request.args.get('sort', 'time_seconds')
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

