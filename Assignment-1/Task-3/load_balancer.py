import random
from quart import Quart, jsonify, Response
import asyncio
import aiohttp
import os
import logging
from consistent_hashing import ConsistentHashMap

app = Quart(__name__)
PORT = 5000
currServer = 100000
server_to_id = {}
id_to_server = {}
nservers = 0
requestId = 100000
logging.basicConfig(level=logging.DEBUG)
map = ConsistentHashMap()

def spawn_server(serverName=None):
    serverId = server_to_id.get(serverName)
    if serverId == None:
        currServer += 1
        serverId = currServer
    containerName = serverName = serverName | f'server{serverId}'
    res = os.popen(f"sudo docker run --name {containerName} --network net1 -e SERVER_ID={serverId} -d server").read()
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
    
async def periodic_heatbeat_check(interval=1):
    app.logger.info("Starting periodic heartbeat check")
    while True:
        deadServerList=[]
        tasks = [check_heartbeat(serverName) for serverName in server_to_id.keys()]
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
        async with aiohttp.ClientSession() as client_session:
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
def add_container(payload):

    if payload["n"] is None or payload["hostnames"] is None : 
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
        return jsonify(message=f"<ERROR> Couldn't add server", status="failure"), 400
    return replicas_list

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



@app.route('/rm', methods=['DELETE'])
def remove_container():
    pass
  
    
@app.after_serving
async def cleanup():
    app.logger.info("Stopping the load balancer")

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

