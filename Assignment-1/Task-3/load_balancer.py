import random
from quart import Quart, jsonify, Response, request
import asyncio
import aiohttp
import os
import logging
import copy
from consistent_hashing import ConsistentHashMap

app = Quart(__name__)
PORT = 5000
currServer = 100000
server_to_id = {}
id_to_server = {}
nservers = 0
logging.basicConfig(level=logging.DEBUG)
map = ConsistentHashMap()

def spawn_server(serverName=None):
    global currServer
    global nservers
    serverId = server_to_id.get(serverName)
    if serverId == None:
        currServer += 1
        serverId = currServer
    if serverName == None:
        serverName = f'server{serverId}'
    containerName = serverName
    res = os.popen(f"docker run --name {containerName} --network net1 --network-alias {containerName} -e SERVER_ID={containerName} -d server").read()
    if res == "":
        app.logger.error(f"Error while spawning {containerName}")
        return False
    else:
        app.logger.info(f"Spawned {containerName}")
        map.addServer(serverId=serverId)
        server_to_id[serverName] = serverId
        id_to_server[serverId] = serverName
        nservers += 1
        return True
    
async def periodic_heatbeat_check(interval=2):
    global nservers
    app.logger.info("Starting periodic heartbeat check")
    while True:
        server_to_id_temp=copy.deepcopy(server_to_id)
        deadServerList=[]
        tasks = [check_heartbeat(serverName) for serverName in server_to_id_temp.keys()]
        results = await asyncio.gather(*tasks)
        results = zip(server_to_id.keys(),results)
        for serverName,isDown in results:
            if isDown == False:
                app.logger.error(f"Server {serverName} is down")
                map.removeServer(serverId=server_to_id[serverName])
                deadServerList.append(serverName)
        for serverName in deadServerList:
            nservers -= 1
            spawn_server(serverName)
        await asyncio.sleep(interval)

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

@app.route('/rep', methods=['GET'])
def replicas_list():
    message = {
        "N": nservers,
        "replicas": list(server_to_id.keys())
    }
    return jsonify(message=message, status="successful"), 200

@app.route('/add', methods=['POST'])
async def add_container():
    payload = await request.get_json()
    if payload is None or payload["n"] is None or payload["hostnames"] is None : 
        return jsonify(message=f"<ERROR> payload doesn't have 'n' or 'hostnames' field", status="failure"), 400
    if len(payload["hostnames"]) > payload["n"] : 
        return jsonify(message=f"<ERROR> Length of hostname list is more than newly added instances", status="failure"), 400
    prev_count = nservers
    for i in range(payload["n"]) : 
        serverName = None
        if i < len(payload["hostnames"]):
            serverName=payload["hostnames"][i]
        if serverName not in server_to_id:
            spawn_server(serverName=serverName)
    if nservers == prev_count : 
        return jsonify(message=f"<ERROR> Couldn't add any server", status="failure"), 400
    return replicas_list()

@app.route('/<path:path>', methods=['GET'])
async def route_to_server(path):
    serverId = map.getServer(random.randint(100000, 999999))
    serverName = id_to_server[serverId]
    try:
        async with aiohttp.ClientSession() as client_session:
            async with client_session.get(f'http://{serverName}:5000/{path}') as resp:
                content = await resp.read()
                if resp.status != 404:
                    return Response(content, status=resp.status, headers=dict(resp.headers))
                else: 
                    return {"message": f"{content} /{path} endpoint does not exist in server replicas", "status": "failure"}, 400
    except Exception as e:
        return {"message": f"{str(e)} Error in handling request", "status": "failure"}, 400


def remove_container(hostname):
    global nservers
    try:
        serverId = server_to_id[hostname]
        map.removeServer(serverId=serverId)
        server_to_id.pop(hostname)
        id_to_server.pop(serverId)
        os.system(f"docker stop {hostname} && docker rm {hostname}")
        nservers -= 1
    except Exception as e:
        app.logger.error(f"<ERROR> {e} occurred while removing hostname={hostname}")
        raise e 
    app.logger.info(f"Server with hostname={hostname} removed successfully")

@app.route('/rm', methods=['DELETE'])
async def remove_containers():
    global nservers
    payload = await request.get_json()
    if payload is None or payload["n"] is None or payload["hostnames"] is None : 
        return jsonify(message=f"<ERROR> payload doesn't have 'n' or 'hostnames' field", status="failure"), 400
    if len(payload["hostnames"]) > payload["n"] : 
        return jsonify(message=f"<ERROR> Length of hostname list is more than removable instances", status="failure"), 400
    for hostname in payload["hostnames"]:
        if hostname not in server_to_id:
            return jsonify(message=f"<ERROR> {hostname} is not a valid server name", status="failure"), 400
    prev_count = nservers
    random_cnt = payload["n"] - len(payload["hostnames"])
    try:
        for hostname in payload["hostnames"]:
            remove_container(hostname=hostname)
        if random_cnt > 0 :
            remove_keys = random.sample(list(server_to_id.keys()), random_cnt)
            for hostname in remove_keys:
                remove_container(hostname=hostname)
    except Exception as e:
        return jsonify(message=f"<ERROR> {e} occurred while removing", status="failure"), 400
    if nservers == prev_count : 
        return jsonify(message=f"<ERROR> Couldn't remove any server", status="failure"), 400
    return replicas_list()

@app.before_serving
async def startup():
    app.logger.info("Starting the load balancer")
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_heatbeat_check())

if __name__ == '__main__':
    for i in range(1,4):
        currServer += 1
        server_to_id[f'server{i}']=currServer
        id_to_server[currServer] = f'server{i}'
        nservers+=1
        map.addServer(server_to_id[f'server{i}'])
    app.run(debug=False, host='0.0.0.0', port=PORT)

