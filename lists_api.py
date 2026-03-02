from flask import Blueprint, jsonify
import traceback
from sqlalchemy import text

# 1. Create a new Blueprint
lists_bp = Blueprint('lists_api', __name__)

def get_adjustment_fields_sql():
    return """
        (CAST(time_seconds / NULLIF(coeff, 0) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / NULLIF(coeff, 0) AS INTEGER) % 60)::text, 2, '0') AS season_adj_time,
        (CAST(time_seconds / NULLIF((coeff + coeff_event - 1), 0) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / NULLIF((coeff + coeff_event - 1), 0) AS INTEGER) % 60)::text, 2, '0') AS event_adj_time,
        (CAST(time_seconds / NULLIF(age_ratio_male, 0) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / NULLIF(age_ratio_male, 0) AS INTEGER) % 60)::text, 2, '0') AS age_adj_time,
        (CAST(time_seconds / NULLIF(age_ratio_sex, 0) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / NULLIF(age_ratio_sex, 0) AS INTEGER) % 60)::text, 2, '0') AS age_sex_adj_time,
        (CAST(time_seconds / NULLIF(coeff, 0) / NULLIF(age_ratio_male, 0) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / NULLIF(coeff, 0) / NULLIF(age_ratio_male, 0) AS INTEGER) % 60)::text, 2, '0') AS age_season_adj_time,
        (CAST(time_seconds / NULLIF(coeff, 0) / NULLIF(age_ratio_sex, 0) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / NULLIF(coeff, 0) / NULLIF(age_ratio_sex, 0) AS INTEGER) % 60)::text, 2, '0') AS age_sex_season_adj_time,
        (CAST(time_seconds / NULLIF((coeff + coeff_event - 1), 0) / NULLIF(age_ratio_male, 0) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / NULLIF((coeff + coeff_event - 1), 0) / NULLIF(age_ratio_male, 0) AS INTEGER) % 60)::text, 2, '0') AS age_event_adj_time,
        (CAST(time_seconds / NULLIF((coeff + coeff_event - 1), 0) / NULLIF(age_ratio_sex, 0) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / NULLIF((coeff + coeff_event - 1), 0) / NULLIF(age_ratio_sex, 0) AS INTEGER) % 60)::text, 2, '0') AS age_sex_event_adj_time,
        (CAST(time_seconds / NULLIF((coeff + coeff_event - 1) * (age_ratio_sex / NULLIF(age_ratio_male, 0)), 0) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / NULLIF((coeff + coeff_event - 1) * (age_ratio_sex / NULLIF(age_ratio_male, 0)), 0) AS INTEGER) % 60)::text, 2, '0') AS sex_event_adj_time,
        (CAST(time_seconds / NULLIF((age_ratio_sex / NULLIF(age_ratio_male, 0)), 0) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / NULLIF((age_ratio_sex / NULLIF(age_ratio_male, 0)), 0) AS INTEGER) % 60)::text, 2, '0') AS sex_adj_time,
        time_seconds,
        time_seconds / NULLIF(coeff, 0) AS season_adj_time_seconds,
        time_seconds / NULLIF((coeff + coeff_event - 1), 0) AS event_adj_time_seconds,
        time_seconds / NULLIF(age_ratio_male, 0) AS age_adj_time_seconds,
        time_seconds / NULLIF(age_ratio_sex, 0) AS age_sex_adj_time_seconds,
        time_seconds / NULLIF(coeff + coeff_event -1, 0) / NULLIF(age_ratio_male, 0) AS age_event_adj_time_seconds,
        time_seconds / NULLIF(coeff + coeff_event -1, 0) / NULLIF(age_ratio_sex, 0) AS age_sex_event_adj_time_seconds,
        time_seconds / NULLIF((coeff + coeff_event - 1), 0) * (age_ratio_sex / NULLIF(age_ratio_male, 0)) AS sex_event_adj_time_seconds,
        time_seconds / (age_ratio_sex / NULLIF(age_ratio_male, 0)) AS sex_adj_time_seconds
    """

# 2. Define the new API endpoint for the fastest runs list
@lists_bp.route('/api/lists/fastest_runs', methods=['GET'])
def get_fastest_runs_by_athlete():
    """
    API endpoint to get the single fastest run for every athlete.
    """
    from app import db  # Import db here to avoid circular import
    # Ensure `request` is defined even if top-level import was omitted in deployed copy
    from flask import request

    try:
        # Allowed sort columns (whitelist to prevent SQL injection)
        allowed_sorts = {
            'time_seconds',
            'season_adj_time_seconds',
            'event_adj_time_seconds',
            'age_adj_time_seconds',
            'sex_adj_time_seconds',
            'age_sex_adj_time_seconds',
            'age_season_adj_time_seconds',
            'sex_event_adj_time_seconds',
            'age_sex_season_adj_time_seconds',
            'age_event_adj_time_seconds',
            'age_sex_event_adj_time_seconds'
        }

        # Read query params
        sort = request.args.get('sort', 'time_seconds')
        direction = request.args.get('direction', 'asc').lower()
        try:
            limit = int(request.args.get('limit', 1000))
        except (ValueError, TypeError):
            limit = 1000

        # Validate inputs
        if sort not in allowed_sorts:
            return jsonify({
                'error': 'Invalid sort column',
                'allowed': sorted(list(allowed_sorts))
            }), 400

        if direction not in ('asc', 'desc'):
            direction = 'asc'

        # Cap limit to a reasonable maximum
        if limit < 1:
            limit = 1
        elif limit > 10000:
            limit = 10000

        # Build and execute the query using the validated column name and direction
        sql = f"""
            SELECT
                event_code,
                event_date,
                athlete_code,
                position,
                name,
                age_group,
                age_grade,
                club,
                comment,
                time,
                event_name,
                coeff,
                coeff_event,
                age_ratio_male,
                age_ratio_sex,
                {get_adjustment_fields_sql()}
            FROM athlete_pbs
            ORDER BY {sort} {direction.upper()}
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

