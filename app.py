from flask import Flask
from flask_cors import CORS  # Import CORS
from flask_sqlalchemy import SQLAlchemy


app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Replace the following credentials with your actual database credentials
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://parkrundata_user:m3UE0JWilwRNS1MBVgN2kr0BnIOVZUmH@host:port/dbname'
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
    # add other columns as needed

@app.route('/')
def hello():
    return 'How quickly will this update the front-end?'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
