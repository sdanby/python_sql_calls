import requests
import sqlite3

# Replace with your actual ngrok URL
LOCAL_API_URL_EVENTS = 'http://your-ngrok-url/events'  # Update with your ngrok URL
LOCAL_API_URL_POSITIONS = 'http://your-ngrok-url/eventpositions'  # Update with your ngrok URL

# Connect to Render's SQLite database
render_db_conn = sqlite3.connect('path/to/render/parkrun.db')  # Path to your Render's SQLite DB
render_cursor = render_db_conn.cursor()

# Fetch local events from the local API
local_events = requests.get(LOCAL_API_URL_EVENTS).json()
local_event_positions = requests.get(LOCAL_API_URL_POSITIONS).json()

# Update events table
for local_event in local_events:
    render_cursor.execute('SELECT * FROM events WHERE event_code = ?', (local_event['event_code'],))
    if not render_cursor.fetchone():  # If the event does not exist, insert it
        render_cursor.execute('INSERT INTO events (event_code, event_name) VALUES (?, ?)', 
                              (local_event['event_code'], local_event['event_name']))

# Update eventpositions table
for local_position in local_event_positions:
    render_cursor.execute('SELECT * FROM eventpositions WHERE event_code = ? AND event_date = ? AND position = ?', 
                          (local_position['event_code'], local_position['event_date'], local_position['position']))
    if not render_cursor.fetchone():  # If position does not exist, insert it
        render_cursor.execute('INSERT INTO eventpositions (event_code, event_date, position, name, male_position, male_count, age_group, age_grade, time, club, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                              (local_position['event_code'], local_position['event_date'], local_position['position'], local_position['name'], local_position['male_position'], local_position['male_count'], local_position['age_group'], local_position['age_grade'], local_position['time'], local_position['club'], local_position['comment']))

# Commit the changes
render_db_conn.commit()
render_db_conn.close()
