from flask import Flask, jsonify, request
from mysql.connector.errors import Error
from SQLHandler import SQLHandler
import os
import asyncio 
import aiohttp

app = Flask(__name__)
sql = SQLHandler()
server_name = os.environ['SERVER_NAME']

# secondary_servers = {}
seqNo = 1
all_WAL = {}    # shard to WAL mapping
'''
Schema for individual WAL: List of dicts (Logs which are not commited by atleast one server)
    {
        "seqNo" : seq no,
        "type" : write/upd/del
        "data" : data
    }
'''
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

async def write_primary(shard, data, secondary_servers):

    # First adding in log
    WAL = all_WAL[shard]
    WAL.append({"seqNo" : seqNo, "type" : "write", "data" : data})

    results = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for server in secondary_servers:
            tasks.append(asyncio.create_task(session.post(f'http://{server}:5000/write', json={"shard" : shard, "data" : data, "WAL" : WAL})))
        results = await asyncio.gather(*tasks, return_exceptions=True)
    cnt = 0
    for result in results:
        if not isinstance(result, Exception) and result.status == 200:
            cnt += 1
    if cnt <= len(secondary_servers)/2:
        return jsonify({"message": "Error while writing", "status": "failure"}), 500
    
    # Writing as majority secondary servers have commited
    sql.UseDB(dbname=shard)
    sql.Insert(table_name='studT', rows=data)

    response_data = {"message": "Data entries added",
                     "status": "success"}

    return jsonify(response_data), 200

def handle_secondary(shard, data, primary_WAL):

    WAL = all_WAL[shard]
    # Removing checkpointed entries from WAL
    idx = 0
    for log in WAL:
        if log["seqNo"] >= primary_WAL[0]["seqNo"]:
            break
        else:
            idx += 1
    WAL = WAL[idx:] if idx > 0 else WAL

    # Logging and commiting new entries
    sql.UseDB(dbname=shard)
    for i, log in enumerate(primary_WAL):
        if WAL[-1]["seqNo"] == log["seqNo"]:
            for j, new_log in enumerate(primary_WAL, start=i+1):
                WAL.append(new_log)
                data = new_log["data"]
                if new_log["type"] == "write":
                    sql.Insert(table_name='studT', rows=data)
                elif new_log["type"] == "update":
                    if not sql.Exists(table_name='studT', col="Stud_id", val=data["Stud_id"]):
                        WAL.pop()
                        return jsonify({"message": f"Error during commiting log: {new_log}\nData entry for Stud_id:{data['Stud_id']} not found", "status": "error"}), 404
                    sql.Update(table_name="studT", col="Stud_id", val=data["Stud_id"], data=data)
                else:
                    if not sql.Exists(table_name='studT', col="Stud_id", val=data["Stud_id"]):
                        WAL.pop()
                        return jsonify({"message": f"Error during commiting log: {new_log}\nData entry for Stud_id:{data['Stud_id']} not found", "status": "error"}), 404
                    sql.Delete(table_name='studT', col="Stud_id", val=data["Stud_id"])
    
    response_data = {"checkpointed": WAL[0]["seqNo"]-1,
                     "status": "success"}
    return jsonify(response_data), 200 

@app.route('/write', methods=['POST'])
async def write_data():

    payload = request.get_json()

    shard = payload.get('shard')
    data = payload.get('data')
    secondary_servers = payload.get('sec_servers', None)

    if not shard or not data:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    if secondary_servers:   #if primary server
        return await write_primary(shard=shard, data=data, secondary_servers=secondary_servers)
    else:
        return handle_secondary(shard=shard, data=data, primary_WAL=payload.get("WAL"))
           
async def update_primary(shard, data, secondary_servers):

    # First adding in log
    WAL = all_WAL[shard]
    WAL.append({"seqNo" : seqNo, "type" : "update", "data" : data})

    results = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for server in secondary_servers:
            tasks.append(asyncio.create_task(session.post(f'http://{server}:5000/update', json={"shard" : shard, "data" : data, "WAL" : WAL})))
        results = await asyncio.gather(*tasks, return_exceptions=True)
    cnt = 0
    for result in results:
        if not isinstance(result, Exception) and result.status == 200:
            cnt += 1
    if cnt <= len(secondary_servers)/2:
        return jsonify({"message": "Error while updating", "status": "failure"}), 500
    
    # Updating as majority secondary servers have commited
    sql.UseDB(dbname=shard)
    if not sql.Exists(table_name='studT', col="Stud_id", val=data["Stud_id"]):
        return jsonify({"message": f"Data entry for Stud_id:{data['Stud_id']} not found", "status": "error"}), 404
    sql.Update(table_name='studT', col="Stud_id", val=data["Stud_id"], data=data)

    response_data = {
        "message": f"Data entry for Stud_id:{data['Stud_id']} updated", "status": "success"}

    return jsonify(response_data), 200

@app.route('/update', methods=['PUT'])
async def update_data():

    payload = request.get_json()

    shard = payload.get('shard')
    data = payload.get('data')
    secondary_servers = payload.get('sec_servers', None)

    if not shard or data.get("Stud_id", None) is None or not data:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    if secondary_servers:   #if primary server
        return await update_primary(shard=shard, data=data, secondary_servers=secondary_servers)
    else:
        return handle_secondary(shard=shard, data=data, primary_WAL=payload.get("WAL"))

async def del_primary(shard, data, secondary_servers):
    
    # First adding in log
    WAL = all_WAL[shard]
    WAL.append({"seqNo" : seqNo, "type" : "delete", "data" : data})

    results = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for server in secondary_servers:
            tasks.append(asyncio.create_task(session.post(f'http://{server}:5000/del', json={"shard" : shard, "Stud_id" : data["Stud_id"], "WAL" : WAL})))
        results = await asyncio.gather(*tasks, return_exceptions=True)
    cnt = 0
    for result in results:
        if not isinstance(result, Exception) and result.status == 200:
            cnt += 1
    if cnt <= len(secondary_servers)/2:
        return jsonify({"message": "Error while deleting", "status": "failure"}), 500
    
    # Deleting as majority secondary servers have commited
    sql.UseDB(dbname=shard)
    Stud_id = data["Stud_id"]
    if not sql.Exists(table_name='studT', col="Stud_id", val=Stud_id):
        return jsonify({"message": f"Data entry for Stud_id:{Stud_id} not found", "status": "error"}), 404
    sql.Delete(table_name='studT', col="Stud_id", val=Stud_id)

    response_data = {
        "message": f"Data entry with Stud_id:{Stud_id} removed", "status": "success"}

    return jsonify(response_data), 200

@app.route('/del', methods=['DELETE'])
async def delete_data():
    payload = request.get_json()

    shard = payload.get('shard')
    Stud_id = payload.get('Stud_id')
    secondary_servers = payload.get('sec_servers', None)

    if not shard or Stud_id is None:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    if secondary_servers:   #if primary server
        return await del_primary(shard=shard, data={"Stud_id" : Stud_id}, secondary_servers=secondary_servers)
    else:
        return handle_secondary(shard=shard, data={"Stud_id" : Stud_id}, primary_WAL=payload.get("WAL"))

# @app.route('/setSecondary', methods=['POST'])
# def set_secondary_servers():
#     payload = request.get_json()
#     shard = payload.get("shard")
#     sec_servers = payload.get("sec_servers")

#     if shard is None or sec_servers is None:
#         return jsonify({"message": "Invalid payload", "status": "error"}), 400
    
#     secondary_servers[shard] = sec_servers
#     return '', 200


@app.errorhandler(Exception)
def handle_exception(e):
    app.logger.error(f"Exception: {e}")
    if isinstance(e, Error):
        return jsonify({"message": e.msg, "status": "error"}), 400
    else:
        return jsonify({"message": "Internal server Error: check params", "status": "error"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)