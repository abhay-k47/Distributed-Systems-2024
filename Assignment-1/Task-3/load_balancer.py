from quart import Quart, jsonify
import asyncio
import aiohttp
import os
import logging
from consistent_hashing import ConsistentHashMap

app = Quart(__name__)
PORT = 5000
currServer = 3
serverList = ["server1", "server2", "server3"]
nservers = 3
logging.basicConfig(level=logging.DEBUG)

def spawn_server():
    containerName = f"server{currServer+1}"
    nservers += 1
    currServer += 1
    res = os.popen(f"sudo docker run --name {containerName} --network net1 -e SERVER_ID={containerName} -d server").read()
    if res == "":
        app.logger.error(f"Error while spawning {containerName}")
        return False
    else:
        app.logger.info(f"Spawned {containerName}")
        serverList.append(containerName)
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
        tasks = [check_heartbeat(serverName) for serverName in serverList]
        results = await asyncio.gather(*tasks)
        for i in range(len(results)):
            if results[i] == False:
                app.logger.error(f"Server {serverList[i]} is down")
                del serverList[i]
                nservers -= 1
                spawn_server()
        await asyncio.sleep(interval)

@app.route('/rep', methods=['GET'])
def replicas_list():
    message = {
        "N": nservers,
        "replicas": serverList
    }
    return jsonify(message=message, status="successful"), 200

@app.route('/<path:path>', methods=['GET'])
def route_to_server(path):
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
    
@app.after_serving
async def cleanup():
    app.logger.info("Stopping the load balancer")

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=PORT)

