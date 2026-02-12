from flask import Blueprint, jsonify
from sqlalchemy import text

# 1. Create a new Blueprint
lists_bp = Blueprint('lists_api', __name__)

# 2. Define the new API endpoint for the fastest runs list
@lists_bp.route('/api/lists/fastest_runs', methods=['GET'])
def get_fastest_runs_by_athlete():
    """
    API endpoint to get the single fastest run for every athlete.
    """
    from app import db  # Import db here to avoid circular import
    try:
        # This query finds the row corresponding to the fastest time for each athlete.
        sql_query = text("""
            SELECT
                e.event_code,
                e.event_date,
                e.athlete_code,
                e.position,
                e.name,
                e.age_group,
                e.age_grade,
                e.club,
                e.comment,
                e.time
            FROM eventpositions e
            INNER JOIN (
                SELECT athlete_code, MIN(time) as min_time
                FROM eventpositions
                GROUP BY athlete_code
            ) AS sub
            ON e.athlete_code = sub.athlete_code AND e.time = sub.min_time
            ORDER BY e.time
            LIMIT 1000;
        """)
        
        result_proxy = db.session.execute(sql_query)
        
        # Fetch column names from the result proxy
        column_names = result_proxy.keys()
        
        # Create a list of dictionaries for the JSON response
        results = [dict(zip(column_names, row)) for row in result_proxy.fetchall()]
        
        return jsonify(results)

    except Exception as e:
        # It's good practice to log the error
        print(f"Database error in get_fastest_runs_by_athlete: {e}")
        return jsonify({"error": "Failed to fetch data from the database"}), 500


