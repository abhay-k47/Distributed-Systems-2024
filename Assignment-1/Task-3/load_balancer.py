import random
from quart import Quart, jsonify
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
    res = os.popen(f"sudo docker run --name {containerName} --network net1 -e SERVER_ID={containerName} -d server").read()
    if res == "":
        app.logger.error(f"Error while spawning {containerName}")
        return False
    else:
        app.logger.info(f"Spawned {containerName}")
        server_to_id[serverName] = serverId
        id_to_server[serverId] = serverName
        nservers += 1
        return True

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
                deadServerList.append(serverName)
        for serverName in deadServerList:
            nservers -= 1
            spawn_server(serverName)
        await asyncio.sleep(interval)

@app.route('/rep', methods=['GET'])
def replicas_list():
    message = {
        "N": nservers,
        "replicas": list(server_to_id.keys())
    }
    return jsonify(message=message, status="successful"), 200

@app.route('/<path:path>', methods=['GET'])
async def route_to_server(path):
    serverId = map.getServer(random.randint(100000, 999999))
    # serverName = id_to_server[serverId]
    # try:
    #     async with aiohttp.ClientSession() as client_session:
    #             async with client_session.get(f'http://{serverName}:5000/{path}') as resp:
    #                 if resp.status == 200:
    #                     return True
    #                 else:
    #                     return False       
    # except Exception as e:
        
    message = f"path: {path}"
    return jsonify(message=message, status="successful"), 200

@app.route('/add', methods=['POST'])
def add_container():
    pass

@app.route('/rm', methods=['DELETE'])
def remove_container():
    pass

@app.before_serving
async def startup():
    app.logger.info("Starting the load balancer")
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_heatbeat_check())
    for i in range(1,4):
        currServer+=1
        server_to_id[f'server{i}']=currServer
        id_to_server[currServer] = f'server{i}'
        nservers+=1
        map.addServer(server_to_id[f'server{i}'])
        
    
    
@app.after_serving
async def cleanup():
    app.logger.info("Stopping the load balancer")

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=PORT)

