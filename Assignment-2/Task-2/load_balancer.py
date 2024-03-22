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
    app.logger.info(f"Configuring {serverName}")
    await asyncio.sleep(30)
    async with aiohttp.ClientSession() as session:
        payload = {"schema": schema, "shards": shards}
        async with session.post(f'http://{serverName}:5000/config', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

# gets the shard data from available server       
async def get_shard_data(shard):
    print(f"Getting shard data for {shard} from available servers")
    serverId = shard_hash_map[shard].getServer(random.randint(1000000, 1000000))
    print(f"ServerId for shard {shard} is {serverId}")
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
        newserver = True
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

            if not newserver:
                await restore_shards(serverName, shardList)
                app.logger.info(f"Restored shards for {containerName}")

            for shard in shardList:
                shard_hash_map[shard].addServer(serverId)
                shard_to_servers.setdefault(shard, []).append(serverName)
            
            id_to_server[serverId] = serverName
            server_to_id[serverName] = serverId
            servers_to_shard[serverName] = shardList

            app.logger.info(f"Updated metadata for {containerName}")
        except Exception as e:
            app.logger.error(f"Error while spawning {containerName}, got exception {e}")
            return False, ""
        
        return True, serverName
    
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
                del servers_to_shard[serverName]
        for serverName in deadServerList:
            await spawn_server(serverName, shardList)
        await asyncio.sleep(interval)

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
    
    global shardT
    global prefix_shard_sizes

    shardT = shards
    prefix_shard_sizes = [0]
    for shard in shards:
        prefix_shard_sizes.append(prefix_shard_sizes[-1] + shard["Shard_size"])

    spawned_servers = []

    # *2 would also work fine
    shards = shards*3
    if not servers:
        for i in range(n):
            nshards = len(shards)//n
            spawned, server = await spawn_server(None, shards[i:i+nshards], schema)
            if spawned:
                spawned_servers.append(server)
        servers = {}

    for server, shardList in servers.items():
        spawned, _ = await spawn_server(server, shardList, schema)
        if spawned:
            spawned_servers.append(server)

    if len(spawned_servers) == 0:
        return jsonify({"message": "No servers spawned", "status": "failure"}), 500
    
    if len(spawned_servers) != n:
        return jsonify({"message": f"Spawned only {spawned_servers} servers", "status": "success"}), 200
    
    return jsonify({"message": "Configured Database", "status": "success"}), 200

@app.route('/status', methods=['GET'])
def status():
    servers = servers_to_shard
    shards = shardT
    N = len(servers)
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
    
    for server in servers:
        if server in server_to_id:
            return jsonify(message=f"<ERROR> {server} already exists", status="failure"), 400
        
    if not new_shards:
        new_shards = []

    for shardData in new_shards:
        shard_size = shardData["Shard_size"]
        shardT.append(shardData)
        prefix_shard_sizes.append(prefix_shard_sizes[-1] + shard_size)

    spawned_servers = []
    for server, shardList in servers.items():
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9_.-]*$', server):
            spawned, server = await spawn_server(None, shardList)
        else:
            spawned, _ = await spawn_server(server, shardList)
        if spawned:
            spawned_servers.append(server)

    if len(spawned_servers) == 0:
        return jsonify({"message": "No servers spawned", "status": "failure"}), 500
    
    return jsonify({"message": f"Add {', '.join(spawned_servers)} servers", "status": "success"}), 200


def remove_container(hostname):
    try:
        serverId = server_to_id[hostname]
        shardList = servers_to_shard[hostname]
        for shard in shardList:
            shard_hash_map[shard].removeServer(serverId)
        del servers_to_shard[hostname]
        available_servers.append(serverId)
        server_to_id.pop(hostname)
        id_to_server.pop(serverId)
        os.system(f"docker stop {hostname} && docker rm {hostname}")
    except Exception as e:
        app.logger.error(f"<ERROR> {e} occurred while removing hostname={hostname}")
        raise e 
    app.logger.info(f"Server with hostname={hostname} removed successfully")


@app.route('/rm', methods=['DELETE'])
async def remove_servers():
    payload = await request.get_json()
    n = payload.get("n")
    servers = payload.get("servers")
    
    if not n or not servers:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400

    for server in servers:
        if server not in server_to_id:
            return jsonify(message=f"<ERROR> {server} is not a valid server name", status="failure"), 400

    random_cnt = n - len(servers)
    remove_keys = []
    try:
        for server in servers:
            remove_container(hostname=server)
        if random_cnt > 0 :
            remove_keys = random.sample(list(server_to_id.keys()), random_cnt)
            for server in remove_keys:
                remove_container(hostname=server)
    except Exception as e:
        return jsonify(message=f"<ERROR> {e} occurred while removing", status="failure"), 400
    
    remove_keys.extend(servers)
    return jsonify({"message": {"N": len(servers_to_shard), "servers": remove_keys}, "status": "success"}), 200

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
                for server in shard_to_servers[shard]:
                    app.logger.info(f"Writing to {server} for shard {shard}")
                    payload = {"shard": shard, "curr_idx": 1, "data": data}
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
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_heatbeat_check())

@app.after_serving
async def cleanup():
    app.logger.info("Stopping the load balancer")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=False)