from quart import Quart, jsonify
import asyncio
import aiohttp
import os
import logging

app = Quart(__name__)
PORT = 5000
currServer = 0
serverList = ['localhost']
nservers = 0
logging.basicConfig(level=logging.DEBUG)

def spawn_server():
    pass

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
                del serverList[i]
                spawn_server()
        await asyncio.sleep(interval)

@app.route('/rep', methods=['GET'])
def replicas_list():
    message = f"List of replicas: {serverList}"
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

