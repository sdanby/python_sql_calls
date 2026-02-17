from flask import Blueprint, jsonify
from sqlalchemy import text

# 1. Create a new Blueprint
lists_bp = Blueprint('lists_api', __name__)

def get_adjustment_fields_sql():
    return """
        (CAST(time_seconds / coeff AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / coeff AS INTEGER) % 60)::text, 2, '0') AS season_adj_time,
        (CAST(time_seconds / (coeff + coeff_event - 1) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / (coeff + coeff_event - 1) AS INTEGER) % 60)::text, 2, '0') AS event_adj_time,
        (CAST(time_seconds / age_ratio_male AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / age_ratio_male AS INTEGER) % 60)::text, 2, '0') AS age_adj_time,
        (CAST(time_seconds / age_ratio_sex AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / age_ratio_sex AS INTEGER) % 60)::text, 2, '0') AS age_sex_adj_time,
        (CAST(time_seconds / coeff / age_ratio_male AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / coeff / age_ratio_male AS INTEGER) % 60)::text, 2, '0') AS age_season_adj_time,
        (CAST(time_seconds / coeff / age_ratio_sex AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / coeff / age_ratio_sex AS INTEGER) % 60)::text, 2, '0') AS age_sex_season_adj_time,
        (CAST(time_seconds / (coeff + coeff_event - 1) / age_ratio_male AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / (coeff + coeff_event - 1) / age_ratio_male AS INTEGER) % 60)::text, 2, '0') AS age_event_adj_time,
        (CAST(time_seconds / (coeff + coeff_event - 1) / age_ratio_sex AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / (coeff + coeff_event - 1) / age_ratio_sex AS INTEGER) % 60)::text, 2, '0') AS age_sex_event_adj_time,
        (CAST(time_seconds / (coeff + coeff_event - 1) / (age_ratio_sex / age_ratio_male) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / (coeff + coeff_event - 1) / (age_ratio_sex / age_ratio_male) AS INTEGER) % 60)::text, 2, '0') AS sex_event_adj_time,
        (CAST(time_seconds / (age_ratio_sex / age_ratio_male) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / (age_ratio_sex / age_ratio_male) AS INTEGER) % 60)::text, 2, '0') AS sex_adj_time,
        time_seconds,
        time_seconds / coeff AS season_adj_time_seconds,
        time_seconds / (coeff + coeff_event - 1) AS event_adj_time_seconds,
        time_seconds / age_ratio_male AS age_adj_time_seconds,
        time_seconds / age_ratio_sex AS age_sex_adj_time_seconds,
        time_seconds / coeff / age_ratio_male AS age_season_adj_time_seconds,
        time_seconds / coeff / age_ratio_sex AS age_sex_season_adj_time_seconds,
        time_seconds / (coeff + coeff_event - 1) / age_ratio_male AS age_event_adj_time_seconds,
        time_seconds / (coeff + coeff_event - 1) / age_ratio_sex AS age_sex_event_adj_time_seconds
    """

# 2. Define the new API endpoint for the fastest runs list
@lists_bp.route('/api/lists/fastest_runs', methods=['GET'])
def get_fastest_runs_by_athlete():
    """
    API endpoint to get the single fastest run for every athlete.
    """
    from app import db  # Import db here to avoid circular import
    try:
        # This query finds the row corresponding to the fastest time for each athlete.
        # This query reads directly from the pre-calculated and indexed materialized view for maximum performance.
        sql_query = text("""
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
        ORDER BY time_seconds ASC
        LIMIT 1000;
        """)
        result_proxy = db.session.execute(sql_query)
        column_names = result_proxy.keys()
        results = [dict(zip(column_names, row)) for row in result_proxy.fetchall()]
        db.session.commit()  # <-- Add this line
        return jsonify(results)
    except Exception as e:
        db.session.rollback()  # <-- Add this line for safety
        print(f"Database error in get_fastest_runs_by_athlete: {e}")
        return jsonify({"error": "Failed to fetch data from the database"}), 500

def get_adjustment_fields_sql():
    return """
        (CAST(time_seconds / coeff AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / coeff AS INTEGER) % 60)::text, 2, '0') AS season_adj_time,
        (CAST(time_seconds / (coeff + coeff_event - 1) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / (coeff + coeff_event - 1) AS INTEGER) % 60)::text, 2, '0') AS event_adj_time,
        (CAST(time_seconds / age_ratio_male AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / age_ratio_male AS INTEGER) % 60)::text, 2, '0') AS age_adj_time,
        (CAST(time_seconds / age_ratio_sex AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / age_ratio_sex AS INTEGER) % 60)::text, 2, '0') AS age_sex_adj_time,
        (CAST(time_seconds / coeff / age_ratio_male AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / coeff / age_ratio_male AS INTEGER) % 60)::text, 2, '0') AS age_season_adj_time,
        (CAST(time_seconds / coeff / age_ratio_sex AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / coeff / age_ratio_sex AS INTEGER) % 60)::text, 2, '0') AS age_sex_season_adj_time,
        (CAST(time_seconds / (coeff + coeff_event - 1) / age_ratio_male AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / (coeff + coeff_event - 1) / age_ratio_male AS INTEGER) % 60)::text, 2, '0') AS age_event_adj_time,
        (CAST(time_seconds / (coeff + coeff_event - 1) / age_ratio_sex AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / (coeff + coeff_event - 1) / age_ratio_sex AS INTEGER) % 60)::text, 2, '0') AS age_sex_event_adj_time,
        (CAST(time_seconds / (coeff + coeff_event - 1) / (age_ratio_sex / age_ratio_male) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / (coeff + coeff_event - 1) / (age_ratio_sex / age_ratio_male) AS INTEGER) % 60)::text, 2, '0') AS sex_event_adj_time,
        (CAST(time_seconds / (age_ratio_sex / age_ratio_male) AS INTEGER) / 60)::text || ':' || lpad((CAST(time_seconds / (age_ratio_sex / age_ratio_male) AS INTEGER) % 60)::text, 2, '0') AS sex_adj_time,
        time_seconds,
        time_seconds / coeff AS season_adj_time_seconds,
        time_seconds / (coeff + coeff_event - 1) AS event_adj_time_seconds,
        time_seconds / age_ratio_male AS age_adj_time_seconds,
        time_seconds / age_ratio_sex AS age_sex_adj_time_seconds,
        time_seconds / coeff / age_ratio_male AS age_season_adj_time_seconds,
        time_seconds / coeff / age_ratio_sex AS age_sex_season_adj_time_seconds,
        time_seconds / (coeff + coeff_event - 1) / age_ratio_male AS age_event_adj_time_seconds,
        time_seconds / (coeff + coeff_event - 1) / age_ratio_sex AS age_sex_event_adj_time_seconds
    """
