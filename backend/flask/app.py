import os

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
import pymongo

app = Flask(__name__)
CORS(app)

_here = os.path.dirname(os.path.abspath(__file__))
_repo_root = os.path.abspath(os.path.join(_here, '..', '..'))
load_dotenv(os.path.join(_repo_root, '.env'))
load_dotenv(os.path.join(_here, '.env'))

def _mongo_uri():
    explicit = os.getenv('MONGO_URI')
    if explicit:
        return explicit
    user = os.getenv('MONGO_USERNAME') or os.getenv('USERNAME')
    password = os.getenv('MONGO_PASSWORD') or os.getenv('PASSWORD')
    host = os.getenv('MONGO_HOST') or os.getenv('HOST')
    database = os.getenv('MONGO_DATABASE') or os.getenv('DATABASE')
    return (
        'mongodb+srv://'
        + user
        + ':'
        + password
        + '@'
        + host
        + '/'
        + database
        + '?retryWrites=true&w=majority'
    )


mongoClient = pymongo.MongoClient(_mongo_uri())
db = mongoClient.get_database('wikiStats')
daywise_changes = db.daywise_changes


@app.route('/')
def hello():
    return 'Hello World!'


@app.route('/hourlyChangesBarChart')
def hourlyChanges():
    day = request.args.get('day')
    if not day:
        return jsonify({'error': 'Missing day query parameter.'})

    try:
        result = daywise_changes.find_one(
            {'day': day}, projection={'_id': 0, 'day': 0}
        )
    except Exception:
        return jsonify({'error': 'Invalid query or database error.'})

    if result is None:
        return jsonify({'error': 'Day of week not found in database.'})

    payload = {'labels': list(result.keys()), 'data': list(result.values())}
    return jsonify(payload)


if __name__ == '__main__':
    app.run()
