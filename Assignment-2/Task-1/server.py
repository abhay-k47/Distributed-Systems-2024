from flask import Flask, jsonify, request
from mysql.connector.errors import Error
from SQLHandler import SQLHandler
import os

app = Flask(__name__)
sql = SQLHandler()
server_name = os.environ['SERVER_NAME']


@app.route('/config', methods=['POST'])
def configure_server():
    payload = request.get_json()
    schema = payload.get('schema')
    shards = payload.get('shards')

    if not schema or not shards:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if 'columns' not in schema or 'dtypes' not in schema or len(schema['columns']) != len(schema['dtypes']) or len(schema['columns']) == 0:
        return jsonify({"message": "Invalid schema", "status": "error"}), 400

    for shard in shards:
        sql.UseDB(dbname=shard)
        sql.CreateTable(
            tabname='studT', columns=schema['columns'], dtypes=schema['dtypes'], prikeys=['Stud_id'])

    response_message = f"{', '.join(f'{server_name}:{shard}' for shard in shards)} configured"
    response_data = {"message": response_message, "status": "success"}

    return jsonify(response_data), 200


@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return '', 200


@app.route('/copy', methods=['GET'])
def copy_data():

    payload = request.get_json()

    shards = payload.get('shards')

    if not shards:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    response_data = {}
    for shard in shards:
        if not sql.hasDB(dbname=shard):
            return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404
        sql.UseDB(dbname=shard)
        response_data[shard] = sql.Select(table_name='studT')
    response_data["status"] = "success"

    return jsonify(response_data), 200


@app.route('/read', methods=['POST'])
def read_data():

    payload = request.get_json()

    shard = payload.get('shard')
    Stud_id = payload.get('Stud_id')

    if not shard or not Stud_id or 'low' not in Stud_id or 'high' not in Stud_id:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    sql.UseDB(dbname=shard)
    data = sql.Select(table_name="studT", col="Stud_id",
                      low=Stud_id['low'], high=Stud_id['high'])

    response_data = {"data": data, "status": "success"}

    return jsonify(response_data), 200


@app.route('/write', methods=['POST'])
def write_data():

    payload = request.get_json()

    shard = payload.get('shard')
    curr_idx = payload.get('curr_idx')
    data = payload.get('data')

    if not shard or curr_idx is None or not data:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    sql.UseDB(dbname=shard)
    sql.Insert(table_name='studT', rows=data)

    curr_idx += len(data)

    response_data = {"message": "Data entries added",
                     "curr_idx": curr_idx, "status": "success"}

    return jsonify(response_data), 200


@app.route('/update', methods=['PUT'])
def update_data():

    payload = request.get_json()

    shard = payload.get('shard')
    Stud_id = payload.get('Stud_id')
    data = payload.get('data')

    if not shard or Stud_id is None or not data:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    sql.UseDB(dbname=shard)
    if not sql.Exists(table_name='studT', col="Stud_id", val=Stud_id):
        return jsonify({"message": f"Data entry for Stud_id:{Stud_id} not found", "status": "error"}), 404
    sql.Update(table_name='studT', col="Stud_id", val=Stud_id, data=data)

    response_data = {
        "message": f"Data entry for Stud_id:{Stud_id} updated", "status": "success"}

    return jsonify(response_data), 200


@app.route('/del', methods=['DELETE'])
def delete_data():
    payload = request.get_json()

    shard = payload.get('shard')
    Stud_id = payload.get('Stud_id')

    if not shard or Stud_id is None:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    sql.UseDB(dbname=shard)
    if not sql.Exists(table_name='studT', col="Stud_id", val=Stud_id):
        return jsonify({"message": f"Data entry for Stud_id:{Stud_id} not found", "status": "error"}), 404
    sql.Delete(table_name='studT', col="Stud_id", val=Stud_id)

    response_data = {
        "message": f"Data entry with Stud_id:{Stud_id} removed", "status": "success"}

    return jsonify(response_data), 200


@app.errorhandler(Exception)
def handle_exception(e):
    app.logger.error(f"Exception: {e}")
    if isinstance(e, Error):
        return jsonify({"message": e.msg, "status": "error"}), 400
    else:
        return jsonify({"message": "Internal server Error: check params", "status": "error"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)