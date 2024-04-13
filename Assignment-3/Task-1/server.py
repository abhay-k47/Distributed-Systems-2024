from flask import Flask, jsonify, request
# from mysql.connector.errors import Error
from SQLHandler import SQLHandler
import os
import logging
import asyncio 
import aiohttp

app = Flask(__name__)
sql = SQLHandler()
server_name = os.environ['SERVER_NAME']

logging.basicConfig(filename=f"WAL_{server_name}.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

secondary_servers = {}

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
async def write_data():

    payload = request.get_json()

    shard = payload.get('shard')
    data = payload.get('data')

    if not shard or not data:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    logger.info(f"Write Request received for data={data}")
    # if it is primary server for this shard
    if len(secondary_servers["shard"]) :
        logger.info(f"Sending write request for data={data}")

        async with aiohttp.ClientSession() as session:
            tasks = []
            for server in secondary_servers[shard]:
                tasks.append(asyncio.create_task(session.post(f'http://{server}:5000/write', json=payload)))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        cnt = 0
        for result in results:
            if not isinstance(result, Exception) and result.status == 200:
                cnt += 1
        if cnt <= len(secondary_servers["shard"])/2:
            return jsonify({"message": "Error while writing", "status": "failure"}), 500


    sql.UseDB(dbname=shard)
    sql.Insert(table_name='studT', rows=data)

    response_data = {"message": "Data entries added",
                     "status": "success"}

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

@app.route('/setSecondary', methods=['POST'])
def set_secondary_servers():
    payload = request.get_json()
    shard = payload.get("shard")
    sec_servers = payload.get("sec_servers")

    if shard is None or sec_servers is None:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400
    
    secondary_servers[shard] = sec_servers
    return '', 200


@app.errorhandler(Exception)
def handle_exception(e):
    app.logger.error(f"Exception: {e}")
    # if isinstance(e, Error):
    #     return jsonify({"message": e.msg, "status": "error"}), 400
    # else:
    #     return jsonify({"message": "Internal server Error: check params", "status": "error"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)