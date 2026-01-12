from flask import Flask, jsonify, request
from flask_cors import CORS  # Make sure to import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Date
from datetime import datetime
from sqlalchemy import func
from sqlalchemy import text # Import text from SQLAlchemy
import re
#from consistency import get_parkrun_data

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Replace the following credentials with your actual database credentials
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://parkrundata_user:m3UE0JWilwRNS1MBVgN2kr0BnIOVZUmH@dpg-cs2r25dsvqrc73dpgdd0-a.frankfurt-postgres.render.com:5432/parkrundata'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class ProcessingStatus(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(10), nullable=False)
    updated_at = db.Column(db.DateTime, server_default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())

class Event(db.Model):
    __tablename__ = 'events'
    event_code = db.Column(db.Integer, primary_key=True)
    event_name = db.Column(db.String, nullable=False)
    display_name = db.Column(db.String, nullable=False)

class EventPosition(db.Model):
    __tablename__ = 'eventpositions'
    event_code = db.Column(db.Integer, db.ForeignKey('events.event_code'), primary_key=True)
    event_date = db.Column(db.String, primary_key=True)
    position = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String)
    male_position = db.Column(db.Integer)
    male_count = db.Column(db.Integer)
    age_group = db.Column(db.String)
    age_grade = db.Column(db.String)
    time = db.Column(db.String)
    club = db.Column(db.String)
    comment = db.Column(db.String)
    athlete_code = db.Column(db.String)
    event_eligible_appearances = db.Column(db.Integer)
    time_ratio= db.Column(db.Float)
    adj_time_seconds= db.Column(db.Float)
    adj_time_ratio= db.Column(db.Float)
    event_code_count= db.Column(db.Integer)
    tourist_flag=db.Column(db.String)
    last_event_code_count= db.Column(db.Integer)
    age_ratio_male= db.Column(db.Float)
    age_ratio_sex= db.Column(db.Float)
    super_tourist= db.Column(db.Integer)
    local_time_ratio= db.Column(db.Float)
    adj2_time_seconds= db.Column(db.Float)
    adj2_time_ratio= db.Column(db.Float)
    distinct_courses_long= db.Column(db.Integer)
    last_event_code_count_long= db.Column(db.Integer)
    total_runs_long= db.Column(db.Integer)
    regular=db.Column(db.String)
    returner=db.Column(db.String)
    super_returner=db.Column(db.String)

class ParkrunEvent(db.Model):
    __tablename__ = 'parkrun_events'
    event_code = db.Column(db.Integer, primary_key=True)
    event_date = db.Column(db.String, nullable=False)  # Use String for storing date
    last_position = db.Column(db.Integer)
    volunteers = db.Column(db.Integer)
    event_number = db.Column(db.Integer,primary_key=True)
    coeff = db.Column(db.Float)
    obs = db.Column(db.Integer)
    coeff_event = db.Column(db.Float)
    avg_time = db.Column(db.Float)
    avgtimelim12 = db.Column(db.Float)
    avgtimelim5 = db.Column(db.Float)
    tourist_count = db.Column(db.Integer)
    super_tourist_count = db.Column(db.Integer)
    regulars = db.Column(db.Integer)
    avg_age = db.Column(db.Float)
    first_timers_count = db.Column(db.Integer)
    returners_count = db.Column(db.Integer)
    club_count = db.Column(db.Integer)
    pb_count = db.Column(db.Integer)
    recentbest_count = db.Column(db.Integer)
    eligible_time_count = db.Column(db.Integer)
    unknown_count = db.Column(db.Integer)
    super_returner_count= db.Column(db.Integer)

    def to_dict(self):
       return {
            'event_code': self.event_code,
            'event_date': self.event_date,  # Return the string directly
            'last_position': self.last_position,
            'volunteers': self.volunteers,
            'event_number' : self.event_number,
            'coeff' : self.coeff,
            'obs' : self.obs,
            'coeff_event' : self.coeff_event,
            'avg_time' : self.avg_time,            
            'avgtimelim12' : self.avgtimelim12,           
            'avgtimelim5' : self.avgtimelim5,
            'tourist_count' : self.tourist_count,
            'super_tourist_count' : self.tourist_count,
            'regulars' : self.regulars,
            'avg_age' : self.avg_age,
            'first_timers_count' : self.first_timers_count,
            'returners_count' : self.returners_count,
            'club_count' : self.club_count,
            'pb_count' : self.pb_count,
            'recentbest_count' : self.recentbest_count,
            'eligible_time_count' : self.eligible_time_count,
            'unknown_count' : self.unknown_count,
            'super_returner_count' : self.super_returner_count
        }

#@app.route('/get_parkrun_data', methods=['GET']) 
#def get_parkrun_data_route(): 
#    return get_parkrun_data()

@app.route('/delete_duplicates', methods=['POST']) 
def delete_duplicates(): 
    try: 
        # SQL query to delete rows with position > 10000 and duplicate event_code and event_date 
        delete_query = """ 
        DELETE FROM parkrun_events 
        WHERE event_number > 10000 
        AND (event_code, event_date) IN ( 
            SELECT event_code, event_date 
            FROM parkrun_events 
            GROUP BY event_code, event_date 
            HAVING COUNT(*) > 1 ); 
            """ 
        result = db.session.execute(text(delete_query)) 
        db.session.commit() 
        return jsonify({'message': 'Duplicate rows deleted successfully', 'deleted_rows': result.rowcount}), 200 
    except Exception as e: 
        db.session.rollback() 
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/eventpositions', methods=['GET'])
def get_event_positions():
    event_code = request.args.get('event_code', default=None, type=int)
    event_date = request.args.get('event_date', default=None, type=str)

    print(f"Received event_code: {event_code}, event_date: {event_date}")

    sql = text("""
    SELECT ep.*, a.total_runs
    FROM eventpositions ep
    LEFT JOIN athletes a ON a.athlete_code = ep.athlete_code
    WHERE (:event_code IS NULL OR ep.event_code = :event_code)
      AND (:event_date IS NULL OR ep.event_date = :event_date)
    ORDER BY ep.position
    """)

    params = {'event_code': event_code, 'event_date': event_date}
    result = db.session.execute(sql, params)

    rows = [dict(r) for r in result.fetchall()]

    return jsonify([{
        'event_code': r.get('event_code'),
        'event_date': r.get('event_date'),
        'position': r.get('position'),
        'name': r.get('name'),
        'male_position': r.get('male_position'),
        'male_count': r.get('male_count'),
        'age_group': r.get('age_group'),
        'age_grade': r.get('age_grade'),
        'time': r.get('time'),
        'club': r.get('club'),
        'comment': r.get('comment'),
        'athlete_code': r.get('athlete_code'),
        'event_eligible_appearances': r.get('event_eligible_appearances'),
        'time_ratio': r.get('time_ratio'),
        'adj_time_seconds': r.get('adj_time_seconds'),
        'adj_time_ratio': r.get('adj_time_ratio'),
        'event_code_count': r.get('event_code_count'),
        'tourist_flag': r.get('tourist_flag'),
        'last_event_code_count': r.get('last_event_code_count'),
        'age_ratio_male': r.get('age_ratio_male'),
        'age_ratio_sex': r.get('age_ratio_sex'),
        'super_tourist': r.get('super_tourist'),
        'local_time_ratio': r.get('local_time_ratio'),
        'adj2_time_seconds': r.get('adj2_time_seconds'),
        'adj2_time_ratio': r.get('adj2_time_ratio'),
        'distinct_courses_long': r.get('distinct_courses_long'),
        'last_event_code_count_long': r.get('last_event_code_count_long'),
        'total_runs_long': r.get('total_runs_long'),
        'regular': r.get('regular'),
        'returner': r.get('returner'),
        'super_returner': r.get('super_returner'),
        # new field from athletes table:
        'total_runs': r.get('total_runs')
    } for r in rows])

@app.route('/api/eventpositions', methods=['DELETE'])
def delete_event_positions():
    data = request.get_json()  # Get the JSON payload
    event_code = data.get('event_code')
    event_date = data.get('event_date')

    # Validate input
    if not event_code or not event_date:
        return jsonify({"error": "event_code and event_date are required"}), 400

    try:
        # Delete from eventpositions table
        rows_deleted = db.session.query(EventPosition).filter(
            EventPosition.event_code == event_code,
            EventPosition.event_date == event_date
        ).delete()
        
        db.session.commit()  # Commit changes to the database
        return jsonify({"message": f"{rows_deleted} record(s) deleted from eventpositions."}), 200

    except Exception as e:
        db.session.rollback()  # Rollback in case of error
        return jsonify({"error": str(e)}), 500

@app.route('/api/eventTimeAdjustment', methods=['GET'])
def get_event_time_adjustment():
    event_code = request.args.get('event_code', default=None, type=int)
    event_date = request.args.get('event_date', default=None, type=str)

    print(f"Received event_code: {event_code}, event_date: {event_date}")

    sql = text("""
    WITH tmp_time_adjustment AS (
        SELECT 
            e.event_date,
            e.event_code,
            time,
            athlete_code,
            age_ratio_male,
            age_ratio_sex,
            substring(e.event_date, 7, 4) || chr(45) || substring(e.event_date, 4, 2) || chr(45) || substring(e.event_date, 1, 2) AS formatted_date,
            CASE
                WHEN length(time) - length(replace(time, ':', '')) = 2 THEN
                    CAST(substring(time, 1, strpos(time, ':') - 1) AS INTEGER) * 3600 +
                    CAST(substring(time, strpos(time, ':') + 1, strpos(substring(time, strpos(time, ':') + 1), ':') - 1) AS INTEGER) * 60 +
                    CAST(substring(time, length(time) - 1, 2) AS INTEGER)
                ELSE
                    CAST(substring(time, 1, strpos(time, ':') - 1) AS INTEGER) * 60 +
                    CAST(substring(time, strpos(time, ':') + 1) AS INTEGER)
            END AS time_seconds,
            adj_time_seconds,
            adj2_time_seconds,
            coeff,
            coeff_event
        FROM eventpositions e
        JOIN parkrun_events p ON e.event_code = p.event_code AND e.event_date = p.event_date
        WHERE (:event_code IS NULL OR e.event_code = :event_code)
          AND (:event_date IS NULL OR e.event_date = :event_date)
    )
    SELECT 
        formatted_date,
        event_code,
        athlete_code,
        coeff AS season_adj,
        coeff + coeff_event - 1 AS event_adj,
        age_ratio_male AS age_adj,
        age_ratio_sex / age_ratio_male AS sex_adj,
        time,
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
    FROM tmp_time_adjustment
    -- WHERE athlete_code = '528017'
    ORDER BY age_event_adj_time
    """)

    params = {'event_code': event_code, 'event_date': event_date}
    result = db.session.execute(sql, params)

    rows = [dict(r) for r in result.fetchall()]

    return jsonify(rows)


@app.route('/api/parkrun_events', methods=['GET'])
def get_parkrun_events():
    event_code = request.args.get('event_code', default=None, type=int)
    
    if event_code is not None:
        try:
            event_code = int(event_code)  # Ensure it's an integer
        except ValueError:
            return jsonify({"error": "Invalid event_code"}), 400

    events = ParkrunEvent.query.filter_by(event_code=event_code).all()
    print(f"Filtered event_code: {event_code}, Found {len(events)} events.")  # Debugging line
    
    formatted_events = [event.to_dict() for event in events]
    return jsonify(formatted_events)

@app.route('/api/parkrun_event', methods=['GET'])
def get_parkrun_event():
    # Retrieve event_code, event_date, and event_number from query parameters
    event_code = request.args.get('event_code', default=None, type=int)  # Get event_code
    event_date = request.args.get('event_date', default=None, type=str)  # Get event_date
    event_number = request.args.get('event_number', default=None, type=int)  # Get event_number

    # Validate input
    if event_code is None:
        return jsonify({"error": "event_code is required"}), 400

    if event_date is None and event_number is None:
        return jsonify({"error": "Either event_date or event_number is required"}), 400
    try:
        # Fetch the specific event based on event_code and event_date or event_number
        if event_number is not None:
            event_record = ParkrunEvent.query.filter_by(event_code=event_code, event_number=event_number).first()
        else:
            #event_record = ParkrunEvent.query.filter_by(event_code=event_code, event_date=formatted_event_date).first()
            event_record = ParkrunEvent.query.filter_by(event_code=event_code, event_date=event_date).first()

        if event_record:
            return jsonify(event_record.to_dict()), 200  # Return the found record
            
        else:
            return jsonify({"error": "Event not found for the given code and date/number."}), 404

    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"error": str(e)}), 500


from flask import jsonify, request
from sqlalchemy import func

@app.route('/api/last_positions', methods=['GET'])
def get_last_positions():
    event_code = request.args.get('event_code', default=None, type=int)

    if event_code is None:
        return jsonify({"error": "event_code is required"}), 400

    # Query to get last positions for the specified event_code
    try:
        last_positions_query = (
            db.session.query(
                EventPosition.event_code,
                EventPosition.event_date,  # Get the event date directly
                func.max(EventPosition.position).label('last_position')  # Find the last position for the week
            )
            .filter(EventPosition.event_code == event_code)  # Filter by the given event_code
            .group_by(
                EventPosition.event_code,
                EventPosition.event_date  # Group by event code and event date
            )
            .all()
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not last_positions_query:
        return jsonify({"message": "No records found for this event code"}), 404

    # Prepare the response
    last_positions = [{
        'event_code': code,
        'event_date': date,  # Use event_date directly
        'last_position': last_position
    } for code, date, last_position in last_positions_query]

    return jsonify(last_positions)  # Return the retrieved last positions as JSON

@app.route('/api/parkrun_events', methods=['DELETE'])
def delete_parkrun_events():
    data = request.get_json()  # Get the JSON payload
    event_code = data.get('event_code')
    event_date = data.get('event_date')

    # Validate input
    if not event_code or not event_date:
        return jsonify({"error": "event_code and event_date are required"}), 400

    try:
        # Delete from parkrun_events table
        rows_deleted = db.session.query(ParkrunEvent).filter(
            ParkrunEvent.event_code == event_code,
            ParkrunEvent.event_date == event_date
        ).delete()

        db.session.commit()  # Commit changes to the database
        return jsonify({"message": f"{rows_deleted} record(s) deleted from parkrun_events."}), 200

    except Exception as e:
        db.session.rollback()  # Rollback in case of error
        return jsonify({"error": str(e)}), 500

@app.route('/process_events', methods=['POST'])
def process_events():
    data = request.get_json() 
    if data is None: 
        return jsonify({'error': 'No JSON payload received'}), 400 
    event_code = data.get('event_code') 
    if event_code is None: 
        return jsonify({'error': 'event_code not provided'}), 400
    print(f"process_events -1 event_code = {event_code}")
    print(event_code)
    events = [] 
    if event_code is not None: 
        with db.engine.connect() as connection: 
            result = connection.execute(text("SELECT * FROM parkrun_events WHERE event_code = :event_code"), {"event_code": event_code}) 
            events = [dict(row.items()) for row in result.mappings()] 
            #print(f"Direct SQL Query Fetched events: {events}"
    if not events:
        return jsonify({'error': 'No events found for the specified event code'}), 404

    # Convert fetched events to a list of dictionaries for easier manipulation
    events_data = [{'event_date': event['event_date'], 'event_number': event['event_number']} for event in events]


    # Sort events to ensure they are in the correct order
    events_data.sort(key=lambda x: datetime.strptime(x['event_date'], '%d/%m/%Y'))

    deleted_records = 0
    events_to_delete = set()  # Use a set to prevent duplicates
    print (f"length of events to delete: {events_to_delete}")
    for i in range(len(events_data)):
        current_event = events_data[i]
        #print(f"current_event_number {current_event['event_number']}")
        if current_event['event_number'] > 10000:
            # Case 1: Check if it has the earliest date
            print(f"test {i}, {events_data[i - 1]['event_number']}, {current_event['event_number']}, {events_data[i + 1]['event_number']},{current_event['event_date']}")

            # Case 2: Check if the previous event number is correct
            if i > 0 and i < len(events_data) - 1 and int(events_data[i - 1]['event_number']) + 2 == int(events_data[i + 1]['event_number']):
                events_to_delete.add(tuple(current_event.items()))

    # Convert set back to list of dictionaries
    events_to_delete = [dict(event) for event in events_to_delete]
    print(f"Events to delete: {events_to_delete}")

    # Deleting the identified records
    for event in events_to_delete:
        ParkrunEvent.query.filter_by(event_number=event['event_number'], event_code=event_code).delete()
        deleted_records += 1

    db.session.commit()  # Submit the changes to the database

    return jsonify({'message': 'Processing complete', 'deleted_records': deleted_records}), 200

@app.route('/api/events', methods=['GET'])
def get_events():
    events = Event.query.all()  # Fetching event names and codes
    return jsonify([{
        'event_code': e.event_code,
        'event_name': e.event_name
    } for e in events])  # Return as JSON


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get the current processing status."""
    status_entry = ProcessingStatus.query.first()
    if status_entry:
        return jsonify({'status': status_entry.status}), 200
    else:
        return jsonify({'status': 'not set'}), 404

@app.route('/api/start', methods=['POST'])
def start_processing():
    """Set the processing status to 'running'."""
    status_entry = ProcessingStatus.query.first()
    
    if status_entry:
        status_entry.status = 'running'
    else:
        status_entry = ProcessingStatus(status='running')
        db.session.add(status_entry)
    
    db.session.commit()
    return jsonify({'status': 'started'}), 200

@app.route('/api/stop', methods=['POST'])
def stop_processing():
    """Set the processing status to 'stopped'."""
    status_entry = ProcessingStatus.query.first()
    
    if status_entry:
        status_entry.status = 'stopped'
        db.session.commit()
        return jsonify({'status': 'stopped'}), 200
    else:
        return jsonify({'status': 'not set'}), 404   

@app.route('/')
def hello():
    return 'How quickly will this update the front-end?'

@app.route('/build', methods=['GET'])
def create_tables():
    db.create_all()  # 

@app.route('/api/event-data', methods=['GET'])
def fetch_event_data():
    """Fetch event data from the database and return it as JSON."""
    try:
        # Connect to your database
        conn, cursor, render_db_conn, render_cursor = connections()
        # Define your SQL query
        cursor.execute('''
        WITH first_15_dates AS (
            SELECT DISTINCT event_date 
            FROM parkrun_events
            ORDER BY date(substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2))
            LIMIT 15)
        SELECT event_code, event_date, time, athlete_code 
        FROM eventpositions
        WHERE event_date IN (SELECT event_date FROM first_15_dates)
        ORDER BY athlete_code;
        ''')
         # Fetch all results
        rows = cursor.fetchall()
        # Fetch column names for the output
        columns = [column[0] for column in cursor.description]
        # Convert the list of tuples to a list of dictionaries
        result = [dict(zip(columns, row)) for row in rows]
        # Close the connection
        conn.close()
        # Return the result as JSON
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/results', methods=['GET'])
def get_results():
    """Get most recent results or all results from a supplied start date (inclusive)."""
    try:
        print("Fetching results from the database...")

        # Get limit from query params, default to 15, clamp to max 100
        limit = request.args.get('limit', default=15, type=int)
        limit = max(1, min(limit, 100))  # Prevent abuse

        # Optional start date (YYYY-MM-DD). If provided we'll return all records from that date (inclusive).
        start_date = request.args.get('date', default=None, type=str)
        params = {'limit': limit}

        if start_date:
            # validate format
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                return jsonify({'error': 'date must be YYYY-MM-DD'}), 400

            query = """
                SELECT 
                  fe.event_code,
                  e.event_name,
                  fe.event_date,
                  fe.last_position,
                  fe.volunteers,
                  fe.event_number,
                  fe.coeff,
                  fe.obs,
                  fe.coeff_event,
                  fe.avg_time,
                  fe.avgtimelim12,
                  fe.avgtimelim5,
                  fe.tourist_count,
                  fe.super_tourist_count,
                  fe.regulars,
                  fe.avg_age,
                  fe.first_timers_count,
                  fe.returners_count,
                  fe.club_count,
                  fe.pb_count,
                  fe.recentbest_count,
                  fe.eligible_time_count,
                  fe.unknown_count,
                  fe.super_returner_count
                FROM (
                  SELECT *,
                         substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2) AS formatted_date
                  FROM parkrun_events
                ) fe
                JOIN events e ON fe.event_code = e.event_code
                WHERE fe.formatted_date >= :start_date
                ORDER BY fe.formatted_date DESC, fe.event_code;
            """
            params['start_date'] = start_date
        else:
            # original latest-n-dates query
            query = """
                WITH formatted_events AS (
                  SELECT *,
                         substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2) AS formatted_date
                  FROM parkrun_events
                ),
                
                latest_dates AS (
                  SELECT DISTINCT formatted_date
                  FROM formatted_events
                  ORDER BY formatted_date DESC
                  LIMIT :limit
                )
                
                SELECT 
                  fe.event_code,
                  e.event_name,
                  fe.event_date,
                  fe.last_position,
                  fe.volunteers,
                  fe.event_number,
                  fe.coeff,
                  fe.obs,
                  fe.coeff_event,
                  fe.avg_time,
                  fe.avgtimelim12,
                  fe.avgtimelim5,
                  fe.tourist_count,
                  fe.super_tourist_count,
                  fe.regulars,
                  fe.avg_age,
                  fe.first_timers_count,
                  fe.returners_count,
                  fe.club_count,
                  fe.pb_count,
                  fe.recentbest_count,
                  fe.eligible_time_count,
                  fe.unknown_count,
                  fe.super_returner_count
                FROM formatted_events fe
                JOIN events e ON fe.event_code = e.event_code
                WHERE fe.formatted_date IN (SELECT formatted_date FROM latest_dates)
                ORDER BY fe.formatted_date DESC, fe.event_code;
            """

        result_proxy = db.session.execute(query, params)
        rows = result_proxy.fetchall()
        columns = result_proxy.keys()
        result = [dict(zip(columns, row)) for row in rows]

        print(f"Fetched {len(result)} results from the database.")
        return jsonify(result), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/resultsAll', methods=['GET'])
def get_resultsAll():
    """Get most recent results."""
    try:
        print("Fetching results from the database...")

        query = """
            WITH formatted_events AS (
              SELECT *,
                     substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2) AS formatted_date
              FROM parkrun_events
            )
            
            SELECT 
              fe.event_code,
              e.event_name,
              fe.event_date,
              fe.last_position,
              fe.volunteers,
              fe.event_number,
              fe.coeff,
              fe.obs,
              fe.coeff_event,
              fe.avg_time,
              fe.avgtimelim12,
              fe.avgtimelim5,
              fe.tourist_count,
              fe.super_tourist_count,
              fe.regulars,
              fe.avg_age,
              fe.first_timers_count,
              fe.returners_count,
              fe.club_count,
              fe.pb_count,
              fe.recentbest_count,
              fe.eligible_time_count,
              fe.unknown_count,
              fe.super_returner_count
            FROM formatted_events fe
            JOIN events e ON fe.event_code = e.event_code
            ORDER BY fe.formatted_date DESC, fe.event_code;
        """

        result_proxy = db.session.execute(query)
        rows = result_proxy.fetchall()
        columns = result_proxy.keys()
        result = [dict(zip(columns, row)) for row in rows]

        print(f"Fetched {len(result)} results from the database.")
        return jsonify(result), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/eventinfo', methods=['GET'])
def get_event_info():
    event_number = request.args.get('event_number', type=int)
    event_code = request.args.get('event_code', type=int)
    # accept both event_name and display_name param names
    event_name = request.args.get('event_name', type=str) or request.args.get('display_name', type=str)
    event_date = request.args.get('event_date', type=str)

    if not event_date or (event_number is None and event_code is None and not event_name):
        return jsonify({"error": "Provide event_date and one of event_number, event_code or event_name"}), 400

    # Build date variants to try (keep original first)
    dates_to_try = [event_date]
    try:
        if re.match(r'^\d{4}-\d{2}-\d{2}$', event_date):
            y, m, d = event_date.split('-')
            alt = f"{d}/{m}/{y}"
            if alt not in dates_to_try:
                dates_to_try.append(alt)
        if re.match(r'^\d{2}/\d{2}/\d{4}$', event_date):
            d, m, y = event_date.split('/')
            alt2 = f"{y}-{m}-{d}"
            if alt2 not in dates_to_try:
                dates_to_try.append(alt2)
    except Exception:
        pass

    try:
        # Base query joins parkrun_events with events to get display_name
        q = db.session.query(ParkrunEvent, Event).join(Event, ParkrunEvent.event_code == Event.event_code)

        record = None
        if event_number is not None:
            record = q.filter(ParkrunEvent.event_number == event_number, ParkrunEvent.event_date.in_(dates_to_try)).first()

        if record is None and event_code is not None:
            record = q.filter(ParkrunEvent.event_code == event_code, ParkrunEvent.event_date.in_(dates_to_try)).first()

        if record is None and event_name:
            # case-insensitive match on event_name
            record = q.filter(func.lower(Event.event_name) == func.lower(event_name), ParkrunEvent.event_date.in_(dates_to_try)).first()

        if not record:
            return jsonify({"error": "Event not found"}), 404

        pe, ev = record  # tuple: (ParkrunEvent, Event)
        display = ev.display_name or ev.event_name
        return jsonify({
            "event_number": pe.event_number,
            "event_name": display,
            "event_code": pe.event_code
        }), 200

    except Exception as e:
        app.logger.exception("get_event_info error")
        return jsonify({"error": str(e)}), 500

@app.route('/api/eventby_number', methods=['GET'])
def get_event_by_number():
    """Return event_date and optional event_name for a supplied event_code + event_number.
       Query params: event_code (int), event_number (int)
    """
    event_code = request.args.get('event_code', default=None, type=int)
    event_number = request.args.get('event_number', default=None, type=int)

    if event_code is None or event_number is None:
        return jsonify({"error": "Provide event_code and event_number"}), 400

    try:
        # Use SQLAlchemy to query parkrun_events joined to events for display_name
        q = db.session.query(ParkrunEvent, Event).join(Event, ParkrunEvent.event_code == Event.event_code)
        rec = q.filter(ParkrunEvent.event_code == event_code, ParkrunEvent.event_number == event_number).first()

        if not rec:
            return jsonify({"error": "Event not found"}), 404

        pe, ev = rec  # ParkrunEvent, Event
        display_name = ev.display_name or ev.event_name if ev is not None else None

        return jsonify({
            'event_code': pe.event_code,
            'event_number': pe.event_number,
            'event_date': pe.event_date,
            'event_name': display_name
        }), 200

    except Exception as e:
        app.logger.exception("get_event_by_number error")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)





