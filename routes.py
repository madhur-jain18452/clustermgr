from flask import Blueprint, jsonify

cache_bp = Blueprint('cache', __name__)

from flask import current_app

@cache_bp.route('/cache', methods=['GET'])
def get_cache():
    global_cache = current_app.config.get('gcc')
    if not global_cache:
        return jsonify({"error": "Global Cache not yet initialized"}), 500
    return jsonify(global_cache.summary(print_summary=False)), 200

  