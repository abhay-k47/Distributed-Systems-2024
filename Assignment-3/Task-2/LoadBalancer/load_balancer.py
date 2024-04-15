from bisect import bisect_left, bisect_right
from collections import defaultdict
import random
import re
from typing import Dict
from quart import Quart, jsonify, Response, request
import asyncio
import aiohttp
import os
import logging
from consistent_hashing import ConsistentHashMap
import time
import mysql.connector
from mysql.connector import Error

class SQLHandler:
    def __init__(self):
        self.mydb = None

    def query(self, sql, value=None):
        if self.mydb is None:
            while self.mydb is None:
                try:
                    self.mydb = mysql.connector.connect(
                        host='metadb',
                        port=3306,
                        user='root',
                        password='Chadwick@12',
                        database='MetaDB'
                    )
                except Exception as e:
                    time.sleep(5)
                    logging.error(
                        f"Error while connecting to MetaDB, got exception {e}")
        cursor = self.mydb.cursor()
        cursor.execute(sql, value) if value else cursor.execute(sql)
        res = cursor.fetchall()
        cursor.close()
        return res
    
    def commit(self):
        if self.mydb is not None:
            self.mydb.commit()

    def close(self):
        if self.mydb is not None:
            self.mydb.close()


app = Quart(__name__)
sql = SQLHandler()
logging.basicConfig(level=logging.DEBUG)
PORT = 5000
available_servers = []
shard_hash_map: Dict[str, ConsistentHashMap] = defaultdict(ConsistentHashMap)
metadata_lock = asyncio.Lock()


async def config_server(serverName, schema, shards):
    app.logger.info(f"Configuring {serverName}")
    while await check_heartbeat(serverName) == False:
        await asyncio.sleep(2)
    async with aiohttp.ClientSession() as session:
        payload = {"schema": schema, "shards": shards}
        async with session.post(f'http://{serverName}:5000/config', json=payload) as resp:
            return resp.status == 200


async def get_shard_data(shard):
    serverName = sql.query(f"SELECT s.Server_name FROM MapT m JOIN ServerT s ON m.Server_id=s.Server_id WHERE m.Shard_id={shard} AND m.Is_primary = true")[0]
    app.logger.info(f"Getting shard data for {shard} from primary server: {serverName}")
    async with aiohttp.ClientSession() as session:
        payload = {"shards": [shard]}
        async with session.get(f'http://{serverName}:5000/copy', json=payload) as resp:
            result = await resp.json()
            data = result.get(shard, None)
            return data if resp.status == 200 else None


async def write_shard_data(serverName, shard, data):
    sec_servers = sql.query(f"SELECT s.Server_name FROM MapT m JOIN ServerT s ON m.Server_id=s.Server_id WHERE m.Shard_id={shard} AND m.Is_primary = false")
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "data": data, "sec_servers": sec_servers}
        async with session.post(f'http://{serverName}:5000/write', json=payload) as resp:
            return resp.status == 200
        

async def get_shard_WAL(shard):
    print(f"Getting shard WAL for {shard} from primary server")
    serverName = sql.query(f"SELECT s.Server_name FROM MapT m JOIN ServerT s ON m.Server_id=s.Server_id WHERE m.Shard_id={shard} AND m.Is_primary = true")[0]
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard}
        async with session.get(f'http://{serverName}:5000/get_wal', json=payload) as resp:
            result = await resp.json()
            wal = result.get("WAL", None)
            return wal if resp.status == 200 else None


async def write_shard_WAL(serverName, shard, WAL):
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "WAL": WAL}
        async with session.post(f'http://{serverName}:5000/set_wal', json=payload) as resp:
            return resp.status == 200


async def restore_shards(serverName, shards):
    for shard in shards:
        shard_data = await get_shard_data(shard)
        await write_shard_data(serverName, shard, shard_data)
        shard_WAL = await get_shard_WAL(shard)
        await write_shard_WAL(serverName, shard, shard_WAL)

#  new server if serverName is None, else spawns server with serverName
# first spawns server, configures it, restores shards, then updates the required maps
async def spawn_server(serverName=None, shardList=[], schema={"columns":["Stud_id","Stud_name","Stud_marks"], "dtypes":["Number","String","Number"]}):
    global available_servers

    serverId = available_servers.pop(0)
    if serverName == None:
        serverName = f'server{serverId}'

    containerName = serverName
    res = os.popen(f"docker run --name {containerName} --network net1 --network-alias {containerName} -e SERVER_NAME={containerName} -d server").read()
    if res == "":
        app.logger.error(f"Error while spawning {containerName}")
        return False, ""
    else:
        app.logger.info(f"Spawned {containerName}")
        try:
            await config_server(serverName, schema, shardList)
            app.logger.info(f"Configured {containerName}")
            existing_shards = sql.query(f"SELECT Shard_id FROM MapT WHERE Is_primary = true")
            await restore_shards(serverName, [shard for shard in shardList if shard in existing_shards])
            app.logger.info(f"Restored shards for {containerName}")

            async with metadata_lock:
                for shard in shardList:
                    result=sql.query("SELECT * FROM MapT WHERE Shard_id=%s",(shard))
                    if len(result)==0:
                        sql.query("INSERT INTO MapT (Shard_id,Server_id,Is_primary) VALUES (%s,%s,%s)",(shard,serverId,True))
                    else:
                        sql.query("INSERT INTO MapT (Shard_id,Server_id,Is_primary) VALUES (%s,%s,%s)",(shard,serverId,False))
                    sql.commit()
                    shard_hash_map[shard].addServer(serverId)
                
                sql.query("INSERT INTO ServerT (Server_id,Server_name) VALUES (%s,%s)",(serverId,serverName))
                sql.commit()

            app.logger.info(f"Updated metadata for {containerName}")
        except Exception as e:
            app.logger.error(f"Error while spawning {containerName}, got exception {e}")
            return False, ""
        return True, serverName
    
async def check_heartbeat(serverName):
    try:
        async with aiohttp.ClientSession(trust_env=True) as client_session:
            async with client_session.get(f'http://{serverName}:5000/heartbeat') as resp:
                return resp.status == 200
    except Exception:
        return False

# assuming 3 replicas when shard placement is not mentioned
@app.route('/init', methods=['POST'])
async def init():
    payload = await request.get_json()
    n = payload.get("N")
    schema = payload.get("schema")
    shards = payload.get("shards")
    servers = payload.get("servers")

    if not n or not schema or not shards:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    if 'columns' not in schema or 'dtypes' not in schema or len(schema['columns']) != len(schema['dtypes']) or len(schema['columns']) == 0:
        return jsonify({"message": "Invalid schema", "status": "failure"}), 400
    
    if len(shards) == 0:
        return jsonify({"message": "Invalid shards or servers", "status": "failure"}), 400
    
    query = ""
    for shard in shards:
        query += f"INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size) VALUES ({shard['Stud_id_low']}, {shard['Shard_id']}, {shard['Shard_size']})"
    sql.query(query)
    sql.commit()

    spawned_servers = []

    # *2 would also work fine
    tasks = []
    shards = shards*3
    if not servers:
        for i in range(n):
            nshards = len(shards)//n
            tasks.append(spawn_server(None, shards[i:i+nshards], schema))
        servers = {}

    for server, shardList in servers.items():
        tasks.append(spawn_server(server, shardList, schema))

    results = await asyncio.gather(*tasks)
    for result in results:
        if result[0]:
            spawned_servers.append(result[1])

    if len(spawned_servers) == 0:
        return jsonify({"message": "No servers spawned", "status": "failure"}), 500
    
    if len(spawned_servers) != n:
        return jsonify({"message": f"Spawned only {spawned_servers} servers", "status": "success"}), 200
    
    return jsonify({"message": "Configured Database", "status": "success"}), 200

@app.route('/status', methods=['GET'])
def status():
    serverList = sql.query("SELECT Server_name FROM ServerT")
    shardList = sql.query("SELECT * FROM ShardT")
    N = len(serverList)
    servers = {}
    for server in serverList:
        servers[server] = sql.query(f"SELECT Shard_id FROM MapT WHERE Server_id={server}")
    shards = []
    for shard in shardList:
        primary_server = sql.query(f"SELECT Server_name FROM MapT m JOIN ServerT s ON m.Server_id=s.Server_id WHERE m.Shard_id={shard[1]} AND m.Is_primary = true")[0]
        shards.append({"Stud_id_low": shard[0], "Shard_id": shard[1], "Shard_size": shard[2], "Primary_server": primary_server})
    return jsonify({"N": N, "shards": shards, "servers": servers, "status": "success"}), 200


# if new_shards are empty, then we are just increasing replication factor
@app.route('/add', methods=['POST'])
async def add_servers():
    payload = await request.get_json()
    n = payload.get("n")
    new_shards = payload.get("new_shards")
    servers = payload.get("servers")
    
    if not n or not servers:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    if n!=len(servers):
        return jsonify*{"message": f"<Error> Number of new servers {n} is not equal to newly added instances {len(new_shards)}", "status": "failure"}, 400
    
    serverList = sql.query("SELECT Server_name FROM ServerT")
    for server in servers:
        if server in serverList:
            return jsonify(message=f"<ERROR> {server} already exists", status="failure"), 400
        
    if not new_shards:
        new_shards = []

    for shardData in new_shards:
        sql.query("INSERT INTO ShardT (Stud_id_low,Shard_id,Shard_size) VALUES (%s,%s,%s)",(shardData["Stud_id_low"],shardData["Shard_id"],shardData["Shard_size"]))
        sql.commit()

    spawned_servers = []
    tasks = []
    for server, shardList in servers.items():
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9_.-]*$', server):
            tasks.append(spawn_server(None, shardList))
        else:
            tasks.append(spawn_server(server, shardList))

    results = await asyncio.gather(*tasks)
    for result in results:
        if result[0]:
            spawned_servers.append(result[1])

    if len(spawned_servers) == 0:
        return jsonify({"message": "No servers spawned", "status": "failure"}), 500
    
    return jsonify({"message": f"Add {', '.join(spawned_servers)} servers", "status": "success"}), 200


def remove_container(hostname):
    try:
        serverId = sql.query(f"SELECT Server_id FROM ServerT WHERE Server_name={hostname}")[0]
        shardList = sql.query(f"SELECT Shard_id FROM MapT WHERE Server_id={serverId}")
        for shard in shardList:
            shard_hash_map[shard].removeServer(serverId)
        available_servers.append(serverId)
        os.system(f"docker stop {hostname} && docker rm {hostname}")
        sql.query(f"DELETE FROM ServerT WHERE Server_id={serverId}; DELETE FROM MapT WHERE Server_id={serverId};")
        sql.commit()
    except Exception as e:
        app.logger.error(f"<ERROR> {e} occurred while removing hostname={hostname}")
        raise e 
    app.logger.info(f"Server with hostname={hostname} removed successfully")


@app.route('/rm', methods=['DELETE'])
async def remove_servers():
    payload = await request.get_json()
    n = payload.get("n")
    servers = payload.get("servers")
    
    if not n or not servers or len(servers) > n:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400

    serverList = sql.query("SELECT Server_name FROM ServerT")
    for server in servers:
        if server not in serverList:
            return jsonify(message=f"<ERROR> {server} is not a valid server name", status="failure"), 400
        else:
            serverList.remove(server)

    try:
        if n > len(servers):
            random_servers = random.sample(serverList, n - len(servers))
            servers.extend(random_servers)
        async with aiohttp.clientSession() as session:
            tasks = []
            for server in servers:
                remove_container(hostname=server)
                task = asyncio.create_task(session.put(f'http://shmgr:5000/primary_elect', payload={"server": server}))
                tasks.append(task)
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result, server in zip(results, servers):
                if isinstance(result, Exception) or result.status != 200:
                    app.logger.error(f"Error while removing {server}: {result}")
                    return jsonify({"message": f"Error while removing {server}", "status": "failure"}), 500

    except Exception as e:
        return jsonify(message=f"<ERROR> {e} occurred while removing", status="failure"), 400
    remaining_servers = sql.query("SELECT COUNT(*) FROM ServerT")[0]
    return jsonify({"message": {"N": remaining_servers, "servers": servers}, "status": "success"}), 200

# from here not completely done
@app.route('/read', methods=['POST'])
async def read():
    payload = await request.get_json()
    stud_id = payload.get("Stud_id")
    if not stud_id:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    low = stud_id.get("low")
    high = stud_id.get("high")

    if not low or not high:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    lower_shard_index = bisect_right(prefix_shard_sizes, low)
    upper_shard_index = bisect_left(prefix_shard_sizes, high+1)
    lower_shard_index -= 1
    shardIndex = lower_shard_index
    shards_queried = [shard["Shard_id"] for shard in shardT[lower_shard_index:upper_shard_index]]
    
    data = []

    for shard in shards_queried:
        serverId = shard_hash_map[shard].getServer(random.randint(100000, 1000000))
        server = id_to_server[serverId]
        async with aiohttp.ClientSession() as session:
            spayload = {"shard": shard , "Stud_id": {"low": max(low, prefix_shard_sizes[shardIndex]), "high": min(high, prefix_shard_sizes[shardIndex]+shardT[shardIndex]["Shard_size"]-1)}}
            app.logger.info(f"Reading from {server} for shard {shard} with payload {spayload}")
            async with session.post(f'http://{server}:5000/read', json=spayload) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    data.extend(result.get("data", []))
                else:
                    app.logger.error(f"Error while reading from {server} for shard {shard}")
                    return jsonify({"message": "Error while reading", "status": "failure"}), 500

            shardIndex += 1
                
    return jsonify({"shards_queried": shards_queried, "data": data, "status": "success"}), 200


@app.route('/write', methods=['POST'])
async def write():
    payload = await request.get_json()
    data = payload.get("data")
    if not data:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    shards_to_data = {}

    # can be optimised instead of binary search, by sorting wrt to Stud_id
    for record in data:
        stud_id = record.get("Stud_id")
        shardIndex = bisect_right(prefix_shard_sizes, stud_id)
        if shardIndex == 0 or (shardIndex == len(prefix_shard_sizes) and stud_id >= prefix_shard_sizes[-1]+shardT[-1]["Shard_size"]):
            return jsonify({"message": "Invalid Stud_id", "status": "failure"}), 400
        shard = shardT[shardIndex-1]["Shard_id"]
        shards_to_data[shard] = shards_to_data.get(shard, [])
        shards_to_data[shard].append(record)
    
    for shard, data in shards_to_data.items():
        async with shard_write_lock[shard]:
            async with aiohttp.ClientSession() as session:
                tasks = []
                #write to the server which is the primary one only
                #find the Server_id of the primary server from MapT
                query = "SELECT Server_id FROM MapT WHERE Is_primary = TRUE"
                # Fetching the first result
                server = sql.query(query)[0][0]
                # for server in shard_to_servers[shard]:
                app.logger.info(f"Writing to {server} for shard {shard}")
                payload = {"shard": shard,"data": data,"primary": True}
                task = asyncio.create_task(session.post(f'http://{server}:5000/write', json=payload))
                tasks.append(task)
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        app.logger.error(f"Error while writing to {server} for shard {shard}, got exception {result}")
                        return jsonify({"message": "Error while writing", "status": "failure"}), 500
                    if result.status != 200:
                        app.logger.error(f"Error while writing to {server} for shard {shard}, got status {result.status}")
                        return jsonify({"message": "Error while writing", "status": "failure"}), 500
                    
    return jsonify({"message": f"{len(data)} Data entries added", "status": "success"}), 200

@app.route('/update', methods=['PUT'])
async def update():
    payload = await request.get_json()
    stud_id = payload.get("Stud_id")
    data = payload.get("data")
    if not stud_id or not data:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    stud_name = data.get("Stud_name")
    stud_marks = data.get("Stud_marks")

    if not stud_name and not stud_marks:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    shardIndex = bisect_right(prefix_shard_sizes, stud_id)
    shardIndex -= 1

    shard = shardT[shardIndex]["Shard_id"]

    async with shard_write_lock[shard]:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for server in shard_to_servers[shard]:
                payload = {"shard": shard, "Stud_id": stud_id, "data": data}
                task = asyncio.create_task(session.put(f'http://{server}:5000/update', json=payload))
                tasks.append(task)
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    app.logger.error(f"Error while updating to {server} for shard {shard}, got exception {result}")
                    return jsonify({"message": "Error while updating", "status": "failure"}), 500
                if result.status != 200:
                    app.logger.error(f"Error while updating to {server} for shard {shard}, got status {result.status}")
                    return jsonify({"message": "Error while updating", "status": "failure"}), 500
                
    return jsonify({"message": f"Data entry for Stud_id: {stud_id} updated", "status": "success"}), 200

@app.route('/del', methods=['DELETE'])
async def delete():
    payload = await request.get_json()
    stud_id = payload.get("Stud_id")
    if not stud_id:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    shardIndex = bisect_right(prefix_shard_sizes, stud_id)
    shardIndex -= 1

    shard = shardT[shardIndex]["Shard_id"]

    async with shard_write_lock[shard]:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for server in shard_to_servers[shard]:
                payload = {"shard": shard, "Stud_id": stud_id}
                task = asyncio.create_task(session.delete(f'http://{server}:5000/del', json=payload))
                tasks.append(task)
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    app.logger.error(f"Error while deleting to {server} for shard {shard}, got exception {result}")
                    return jsonify({"message": "Error while deleting", "status": "failure"}), 500
                if result.status != 200:
                    app.logger.error(f"Error while deleting to {server} for shard {shard}, got status {result.status}")
                    return jsonify({"message": "Error while deleting", "status": "failure"}), 500
                
    return jsonify({"message": f"Data entry with Stud_id: {stud_id} removed from all replicas", "status": "success"}), 200

@app.before_serving
async def startup():
    app.logger.info("Starting the load balancer")
    global available_servers
    available_servers = [i for i in range(100000, 1000000)]
    random.shuffle(available_servers)

@app.after_serving
async def cleanup():
    sql.close()
    app.logger.info("Stopping the load balancer")


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=PORT, debug=False)