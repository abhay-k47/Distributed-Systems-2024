from bisect import bisect_right
from collections import defaultdict
import random
from typing import Dict
from quart import Quart, jsonify, Response, request
import asyncio
import aiohttp
import os
import logging
import copy
from consistent_hashing import ConsistentHashMap

app = Quart(__name__)
logging.basicConfig(level=logging.DEBUG)
PORT = 5000

available_servers = []
server_to_id = {}
id_to_server = {}
shard_to_servers = {}
servers_to_shard = {}
prefix_shard_sizes = []
shardT = []
# clientSession = aiohttp.ClientSession() # will optimise this after every other thing works fine

shard_hash_map:Dict[str, ConsistentHashMap] = defaultdict(ConsistentHashMap)
shard_write_lock = defaultdict(lambda: asyncio.Lock())

# configs server for particular schema and shards
async def config_server(serverName, schema, shards):
    async with aiohttp.ClientSession() as session:
        payload = {"schema": schema, "shards": shards}
        async with session.post(f'http://{serverName}:5000/config', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

# gets the shard data from available server       
async def get_shard_data(shard):
    serverId = shard_hash_map[shard]
    serverName = id_to_server[serverId]
    async with aiohttp.ClientSession() as session:
        payload = {"shards": [shard]}
        async with session.get(f'http://{serverName}:5000/copy', json=payload) as resp:
            result = await resp.json()
            data = result.get(shard, None)
            if resp.status == 200:
                return data
            else:
                return None

# writes the shard data into server
async def write_shard_data(serverName, shard, data):
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "curr_idx": 1, "data": data}
        async with session.post(f'http://{serverName}:5000/write', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

# dead server restores shards from other servers
async def restore_shards(serverName, shards):
    for shard in shards:
        shard_data = await get_shard_data(shard)
        await write_shard_data(serverName, shard, shard_data)

# spawns new server if serverName is None, else spawns server with serverName
# if old server is respawned then it restores shards from other servers
# first spawns server, configures it, restores shards, then updates the required maps
async def spawn_server(serverName=None, shardList=[], schema={"columns":["Stud_id","Stud_name","Stud_marks"], "dtypes":["Number","String","Number"]}):
    global available_servers

    newserver = False
    serverId = server_to_id.get(serverName)
    if serverId == None:
        serverId = available_servers.pop(0)
    if serverName == None:
        serverName = f'server{serverId}'
        newserver = True

    containerName = serverName
    res = os.popen(f"docker run --name {containerName} --network net1 --network-alias {containerName} -e SERVER_ID={containerName} -d server").read()
    if res == "":
        app.logger.error(f"Error while spawning {containerName}")
        return False
    else:
        app.logger.info(f"Spawned {containerName}")
        try:
            await config_server(serverName, schema, shardList)
            app.logger.info(f"Configured {containerName}")

            if not newserver:
                await restore_shards(serverName, shardList)
                app.logger.info(f"Restored shards for {containerName}")

            for shard in shardList:
                shard_hash_map[shard].addServer(serverId)
                shard_to_servers.setdefault(shard, []).append(serverId)
            
            id_to_server[serverId] = serverName
            server_to_id[serverName] = serverId
            servers_to_shard[serverName] = shardList
            if newserver:
                available_servers.append(serverId) 

            app.logger.info(f"Updated metadata for {containerName}")
        except Exception as e:
            app.logger.error(f"Error while spawning {containerName}, got exception {e}")
            return False
        
        return True
    
# checks periodic heartbeat of server
async def check_heartbeat(serverName):
    try:
        app.logger.info(f"Checking heartbeat of {serverName}")
        async with aiohttp.ClientSession(trust_env=True) as client_session:
            async with client_session.get(f'http://{serverName}:5000/heartbeat') as resp:
                if resp.status == 200:
                    return True
                else:
                    return False
    except Exception as e:
        app.logger.error(f"Error while checking heartbeat of {serverName}: {e}")
        return False

async def periodic_heatbeat_check(interval=2):
    app.logger.info("Starting periodic heartbeat check")
    while True:
        server_to_id_temp=copy.deepcopy(server_to_id)
        deadServerList=[]
        tasks = [check_heartbeat(serverName) for serverName in server_to_id_temp.keys()]
        results = await asyncio.gather(*tasks)
        results = zip(server_to_id_temp.keys(),results)
        for serverName,isDown in results:
            if isDown == False:
                app.logger.error(f"Server {serverName} is down")
                shardList = []  
                for shard in servers_to_shard[serverName]:
                    shardList.append(shard)
                    shard_hash_map[shard].removeServer(server_to_id[serverName])
                deadServerList.append(serverName)
        for serverName in deadServerList:
            spawn_server(serverName, shardList)
        await asyncio.sleep(interval)

@app.route('/init', methods=['POST'])
async def init():
    payload = await request.get_json()
    n = payload.get("n")
    schema = payload.get("schema")
    shards = payload.get("shards")
    servers = payload.get("servers")

    if not n or not schema or not shards or not servers:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400
    
    if 'columns' not in schema or 'dtypes' not in schema or len(schema['columns']) != len(schema['dtypes']) or len(schema['columns']) == 0:
        return jsonify({"message": "Invalid schema", "status": "error"}), 400
    
    if len(shards) == 0 or len(servers) == 0:
        return jsonify({"message": "Invalid shards or servers", "status": "error"}), 400
    
    global shardT
    global prefix_shard_sizes

    shardT = shards
    prefix_shard_sizes = [0]
    for shard in shards:
        prefix_shard_sizes.append(prefix_shard_sizes[-1] + shard["Shard_size"])
    
    spawned_servers = []
    for server, shardList in servers.items():
        spawned = await spawn_server(server, shardList, schema)
        if spawned:
            spawned_servers.append(server)

    if len(spawned_servers) == 0:
        return jsonify({"message": "No servers spawned", "status": "error"}), 500
    
    if len(spawned_servers) != n:
        return jsonify({"message": f"Spawned only {spawned_servers} servers", "status": "success"}), 200
    
    return jsonify({"message": "Configured Database", "status": "success"}), 200

@app.route('/status', methods=['GET'])
def status():
    servers = servers_to_shard
    shards = shardT
    N = len(servers)
    return jsonify({"N": N, "shards": shards, "servers": servers, "status": "success"}), 200

@app.route('/add', methods=['POST'])
async def add_servers():
    payload = await request.get_json()
    n = payload.get("n")
    new_shards = payload.get("new_shards")
    servers = payload.get("servers")
    

@app.route('/read', methods=['POST'])
async def read():
    payload = await request.get_json()
    stud_id = payload.get("Stud_id")
    if not stud_id:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400
    
    low = stud_id.get("low")
    high = stud_id.get("high")

    if not low or not high:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400
    
    lower_shard_index = bisect_right(prefix_shard_sizes, low)
    upper_shard_index = bisect_right(prefix_shard_sizes, high)
    lower_shard_index -= 1

    shards_queried = [shard["Shard_id"] for shard in shardT[lower_shard_index:upper_shard_index+1]]
    
    data = []

    for shard in shards_queried:
        serverId = shard_hash_map[shard]
        server = id_to_server[serverId]
        async with aiohttp.ClientSession() as session:
            spayload = {"Stud_id": {"low": max(low, prefix_shard_sizes[lower_shard_index]), "high": min(high, prefix_shard_sizes[upper_shard_index])}}
            async with session.post(f'http://{server}:5000/read', json=spayload) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    data.extend(result.get("data", []))
                else:
                    app.logger.error(f"Error while reading from {server} for shard {shard}")
                    return jsonify({"message": "Error while reading", "status": "error"}), 500
                
    return jsonify({"shards_queried": shards_queried, "data": data, "status": "success"}), 200


@app.route('/write', method=["POST"])
async def write():
    payload = await request.get_json()
    data = payload.get("data")
    if not data:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400
    
    shards_to_data = {}

    for record in data:
        stud_id = record.get("Stud_id")
        shardIndex = bisect_right(prefix_shard_sizes, stud_id)
        shard = shardT[shardIndex-1]["Shard_id"]
        shards_to_data[shard] = shards_to_data.get(shard, [])
        shards_to_data[shard].append(record)
    
    for shard, data in shards_to_data.items():
        async with shard_write_lock[shard]:
            async with aiohttp.ClientSession() as session:
                tasks = []
                for server in shard_to_servers[shard]:
                    payload = {"shard": shard, "curr_idx": 1, "data": data}
                    task = asyncio.create_task(session.post(f'http://{server}:5000/write', json=payload))
                    tasks.append(task)
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        app.logger.error(f"Error while writing to {server} for shard {shard}, got exception {result}")
                        return jsonify({"message": "Error while writing", "status": "error"}), 500
                    if result.status != 200:
                        app.logger.error(f"Error while writing to {server} for shard {shard}, got status {result.status}")
                        return jsonify({"message": "Error while writing", "status": "error"}), 500
                    
    return jsonify({"message": f"{len(data)} Data entries added", "status": "success"}), 200

@app.before_serving
async def startup():
    app.logger.info("Starting the load balancer")
    global available_servers
    available_servers = [i for i in range(100000, 1000000)]
    random.shuffle(available_servers)
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_heatbeat_check())

@app.after_serving
async def cleanup():
    app.logger.info("Stopping the load balancer")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=False)