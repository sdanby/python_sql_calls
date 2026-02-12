from flask import Blueprint, jsonify

# 1. Create a new Blueprint
lists_bp = Blueprint('lists_api', __name__)

# 2. Move your list-related routes here
@lists_bp.route('/api/lists/some_list', methods=['GET'])
def get_some_list():
    # Your database logic here...
    data = {"message": "This is some list data"}
    return jsonify(data)

@lists_bp.route('/api/lists/another_list', methods=['GET'])
def get_another_list():
    # Your database logic here...
    data = {"message": "This is another list's data"}
    return jsonify(data)
