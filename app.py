from flask import Flask, jsonify, request
from flask_cors import CORS  # Make sure to import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Date
from datetime import datetime
from sqlalchemy import func
from sqlalchemy import text # Import text from SQLAlchemy
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

    def to_dict(self):
       return {
            'event_code': self.event_code,
            'event_date': self.event_date,  # Return the string directly
            'last_position': self.last_position,
            'volunteers': self.volunteers,
            'event_number' : self.event_number,
            'coeff' : self.coeff,
            'obs' : self.obs,
            'coeff_event' : self.coeff_event

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
    event_code = request.args.get('event_code', default=None, type=int)  # Get event_code from request
    event_date = request.args.get('event_date', default=None, type=str)  # Get event_date from request

    # Print statements for debugging
    print(f"Received event_code: {event_code}, event_date: {event_date}")
    
    query = EventPosition.query

    # Building SQL query based on provided parameters
    if event_code is not None and event_date is not None:
        query = query.filter(EventPosition.event_code == event_code, EventPosition.event_date == event_date)
    elif event_code is not None:
        query = query.filter(EventPosition.event_code == event_code)
    elif event_date is not None:
        query = query.filter(EventPosition.event_date == event_date)
    
    event_positions = query.all()

    # Print the fetched rows for debugging
    #print(f"Fetched rows: {rows}")

    # Return as JSON
    return jsonify([{
        'event_code': ep.event_code,
        'event_date': ep.event_date,
        'position': ep.position,
        'name': ep.name,
        'male_position': ep.male_position,
        'male_count': ep.male_count,
        'age_group': ep.age_group,
        'age_grade': ep.age_grade,
        'time': ep.time,
        'club': ep.club,
        'comment': ep.comment,
        'athlete_code': ep.athlete_code,
    } for ep in event_positions])

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
    """Get most recent results."""
    try:
        print("Fetching results from the database...")

        query = """
            WITH formatted_events AS (
              SELECT *,
                     substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2) AS formatted_date
              FROM parkrun_events
            ),
            ranked_events AS (
              SELECT *,
                     ROW_NUMBER() OVER (
                       PARTITION BY event_code
                       ORDER BY formatted_date DESC
                     ) AS rn
              FROM formatted_events
            )
            SELECT event_code, event_date, last_position, volunteers, event_number, coeff, obs, coeff_event
            FROM ranked_events
            WHERE rn <= 15
            ORDER BY event_code, formatted_date;
        """

        result_proxy = db.session.execute(query)
        rows = result_proxy.fetchall()
        columns = result_proxy.keys()
        result = [dict(zip(columns, row)) for row in rows]

        print(f"Fetched {len(result)} results from the database.")
        return jsonify(result), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
