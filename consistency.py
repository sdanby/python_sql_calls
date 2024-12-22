from flask import Flask, jsonify,request
from database import connections
import pandas as pd 
from datetime import datetime
import logging
#app = Flask(__name__)
conn,cursor,render_db_conn,render_cursor=connections()

# Function to fetch data from both databases 
def fetch_data(event_code): 
# Fetch data from SQLite 
    cursor.execute("SELECT event_date, event_number, last_position, volunteers FROM parkrun_events WHERE event_code = ?", (event_code,)) 
    sqlite_data = cursor.fetchall() 

    # Fetch data from PostgreSQL 
    render_cursor.execute("SELECT event_date, event_number, last_position, volunteers FROM parkrun_events WHERE event_code = %s", (event_code,)) 
    pg_data = render_cursor.fetchall() 

    return sqlite_data, pg_data

# Function to fetch max position for each event date from EventPosition table 
def fetch_max_position(event_code): 
    # Fetch max position from SQLite 
    cursor.execute("SELECT event_date, MAX(position) FROM eventpositions WHERE event_code = ? GROUP BY event_date", (event_code,)) 
    sqlite_max_pos = dict(cursor.fetchall()) 
    
    # Fetch max position from PostgreSQL 
    render_cursor.execute("SELECT event_date, MAX(position) FROM eventpositions WHERE event_code = %s GROUP BY event_date", (event_code,)) 
    pg_max_pos = dict(render_cursor.fetchall()) 
    
    return sqlite_max_pos, pg_max_pos

# Function to create a DataFrame for each event_code 
def create_table(event_code): 
    sqlite_data, pg_data = fetch_data(event_code) 
    sqlite_max_pos, pg_max_pos = fetch_max_position(event_code) 
    
    # Extract event dates and ensure they are unique and sorted 
    event_dates = sorted(set([row[0] for row in sqlite_data + pg_data]), key=lambda date: datetime.strptime(date, '%d/%m/%Y')) 
    
    # Build the DataFrame 
    data = {
        'event_code': [event_code] * len(event_dates),
        'event_date': event_dates, 
        'sd_event_number': [next((row[1] for row in sqlite_data if row[0] == date), None) for date in event_dates], 
        'pd_event_number': [next((row[1] for row in pg_data if row[0] == date), None) for date in event_dates], 
        'sd_last_position': [next((row[2] for row in sqlite_data if row[0] == date), None) for date in event_dates], 
        'pd_last_position': [next((row[2] for row in pg_data if row[0] == date), None) for date in event_dates], 
        'sd_volunteers': [next((row[3] for row in sqlite_data if row[0] == date), None) for date in event_dates], 
        'pd_volunteers': [next((row[3] for row in pg_data if row[0] == date), None) for date in event_dates], 
        'sdE_last_position': [sqlite_max_pos.get(date, None) for date in event_dates], 
        'pdE_last_position': [pg_max_pos.get(date, None) for date in event_dates] 
        }
    
    df = pd.DataFrame(data)

    return df

def get_parkrun_data(): 
    try:
        combined_df = pd.DataFrame() 
        for event_code in range(1, 2): # Assuming event_code ranges from 1 to 25 
            event_df = create_table(event_code) 
            combined_df = pd.concat([combined_df, event_df], ignore_index=True) 
        #return combined_df.to_json(orient='records')
        result = combined_df.to_dict(orient='records') 
        return jsonify(result)
    except Exception as e: 
        logging.error(f"Error fetching parkrun data: {e}") 
        return jsonify({"error": "An error occurred while fetching the data."}), 500
