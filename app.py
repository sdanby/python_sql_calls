from flask import Flask, jsonify, request
from flask_cors import CORS  # Make sure to import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Date
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Replace the following credentials with your actual database credentials
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://parkrundata_user:m3UE0JWilwRNS1MBVgN2kr0BnIOVZUmH@dpg-cs2r25dsvqrc73dpgdd0-a.frankfurt-postgres.render.com:5432/parkrundata'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

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
    event_number = db.Column(db.Integer, primary=True)

    def to_dict(self):
       return {
            'event_code': self.event_code,
            'event_date': self.event_date,  # Return the string directly
            'last_position': self.last_position,
            'volunteers': self.volunteers,
            'event_number': seleft.event_number,
        }
    
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


@app.route('/api/events', methods=['GET'])
def get_events():
    events = Event.query.all()  # Fetching event names and codes
    return jsonify([{
        'event_code': e.event_code,
        'event_name': e.event_name
    } for e in events])  # Return as JSON

@app.route('/')
def hello():
    return 'How quickly will this update the front-end?'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
