from flask import Flask
from flask_cors import CORS  # Import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route('/')
def hello():
    return 'Hello, World!!!!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
