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
import time
import mysql.connector
from mysql.connector import Error

app = Quart(__name__)
logging.basicConfig(level=logging.DEBUG)
PORT = 5000

metadata_lock = asyncio.Lock()

# configs server for particular schema and shards
async def config_server(serverName, schema, shards):
    app.logger.info(f"Configuring {serverName}")
    while True:
        spawned = await check_heartbeat(serverName, log=False)
        if spawned:
            break
        await asyncio.sleep(2)
    async with aiohttp.ClientSession() as session:
        payload = {"schema": schema, "shards": shards}
        async with session.post(f'http://{serverName}:5000/config', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False


# dead server restores shards from other servers
async def restore_shards(serverName, shards):
    for shard in shards:
        shard_data = await get_shard_data(shard)
        await write_shard_data(serverName, shard, shard_data)
# gets the shard data from available server 
      
async def get_shard_data(shard):
    print(f"Getting shard data for {shard} from available servers")
    serverName = None
    async with metadata_lock:
        # serverId = shard_hash_map[shard].getServer(random.randint(1000000, 1000000))
        # fetch a server id from mapT corresponding to the shard
        
        try:
            connection=mysql.connector.connect(
                host='metadb',
                port=3306,
                user='root',
                password='Chadwick@12'
            )
            print("Connected to MySQL: ", connection)
        except Error as e:
            print("Error while connecting to MySQL", e)
        cursor=connection.cursor()
        cursor.execute("USE test_db")
        # fetch serverId from mapT
        cursor.execute(f"SELECT server_id FROM mapT WHERE shard_id={shard}")
        serverId=cursor.fetchone()[0]
        # fetch server name from id_server_map 
        print(f"ServerId for shard {shard} is {serverId}")
        cursor.execute(f"SELECT serverName FROM id_server_map WHERE serverId={serverId}")
        serverName=cursor.fetchone()[0]
        # serverName = id_to_server[serverId]
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
            
# spawns new server if serverName is None, else spawns server with serverName
# if old server is respawned then it restores shards from other servers
# first spawns server, configures it, restores shards, then updates the required maps
async def spawn_server(serverName=None, shardList=[], schema={"columns":["Stud_id","Stud_name","Stud_marks"], "dtypes":["Number","String","Number"]}):
    global available_servers

    newserver = False
    # serverId = server_to_id.get(serverName)
    serverId = None
    try:
        connection=mysql.connector.connect(
            host='metadb',
            port=3306,
            user='root',
            password='Chadwick@12'
        )
        print("Connected to MySQL: ", connection)
    except Error as e:
        print("Error while connecting to MySQL", e)
    cursor=connection.cursor()
    cursor.execute("USE test_db") 
    # get the serverId from serverName using id_server_map
    cursor.execute(f"SELECT serverId FROM id_server_map WHERE serverName='{serverName}'")
    serverId=cursor.fetchone()[0]
    connection.close()
    if serverId == None:
        newserver = True
        async with metadata_lock:
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

            async with metadata_lock:
                for shard in shardList:
                    try:
                        connection=mysql.connector.connect(
                            host='metadb',
                            port=3306,
                            user='root',
                            password='Chadwick@12'
                        )
                        print("Connected to MySQL: ", connection)
                    except Error as e:
                        print("Error while connecting to MySQL", e)
                    cursor=connection.cursor()
                    #use test_db
                    cursor.execute("USE test_db")
                    cursor.execute("SELECT * FROM mapT WHERE shard_id=%s",(shard))
                    #use result of previous query
                    result=cursor.fetchall()
                    if len(result)==0:
                        cursor.execute("INSERT INTO mapT (shard_id,server_id,is_primary) VALUES (%s,%s,%s)",(shard,serverId,True))
                    else:
                        cursor.execute("INSERT INTO mapT (shard_id,server_id,is_primary) VALUES (%s,%s,%s)",(shard,serverId,False))

                    # shard_hash_map[shard].addServer(serverId)
                    # shard_to_servers.setdefault(shard, []).append(serverName)
                try:
                        connection=mysql.connector.connect(
                            host='metadb',
                            port=3306,
                            user='root',
                            password='Chadwick@12'
                        )
                        print("Connected to MySQL: ", connection)
                except Error as e:
                    print("Error while connecting to MySQL", e)
                cursor=connection.cursor()
                #use test_db
                cursor.execute("USE test_db")
                cursor.execute("INSERT INTO id_server_map (server_id,server_name) VALUES (%s,%s)",(serverId,serverName))
                connection.commit()
                connection.close()
                # id_to_server[serverId] = serverName
                # server_to_id[serverName] = serverId
                # servers_to_shard[serverName] = shardList

            app.logger.info(f"Updated metadata for {containerName}")
        except Exception as e:
            app.logger.error(f"Error while spawning {containerName}, got exception {e}")
            return False, ""
        
        return True, serverName
    
# checks periodic heartbeat of server
async def check_heartbeat(serverName, log=True):
    try:
        if(log):
            app.logger.info(f"Checking heartbeat of {serverName}")
        async with aiohttp.ClientSession(trust_env=True) as client_session:
            async with client_session.get(f'http://{serverName}:5000/heartbeat') as resp:
                if resp.status == 200:
                    return True
                else:
                    return False
    except Exception as e:
        if log:
            app.logger.error(f"Error while checking heartbeat of {serverName}: {e}")
        return False

async def periodic_heatbeat_check(interval=2):
    app.logger.info("Starting periodic heartbeat check")
    while True:
        # fetch all the server names from id_server_map
        try:
                        connection=mysql.connector.connect(
                            host='metadb',
                            port=3306,
                            user='root',
                            password='Chadwick@12'
                        )
                        print("Connected to MySQL: ", connection)
        except Error as e:
            print("Error while connecting to MySQL", e)
        cursor=connection.cursor()
        #use test_db
        cursor.execute("USE test_db")
        cursor.execute("SELECT server_name FROM id_server_map")
        # store the server names in a list
        server_to_id_temp = {}
        for server in cursor.fetchall():
            server_to_id_temp[server[0]] = 1
        deadServerList=[]
        tasks = [check_heartbeat(serverName) for serverName in server_to_id_temp.keys()]
        results = await asyncio.gather(*tasks)
        results = zip(server_to_id_temp.keys(),results)
        for serverName,isDown in results:
            if isDown == False:
                app.logger.error(f"Server {serverName} is down")
                shardList = []  
                # find serverId from serverName
                try:
                        connection=mysql.connector.connect(
                            host='metadb',
                            port=3306,
                            user='root',
                            password='Chadwick@12'
                        )
                        # print("Connected to MySQL: ", connection)
                except Error as e:
                    print("Error while connecting to MySQL", e)
                cursor=connection.cursor()
                #use test_db
                cursor.execute("USE test_db")
                cursor.execute("SELECT server_id FROM id_server_map WHERE server_name=%s",(serverName))
                serverId=cursor.fetchone()[0]
                # fetch all shards corresponding to serverId
                cursor.execute("SELECT shard_id FROM mapT WHERE server_id=%s",(serverId))
                shards=cursor.fetchall()
                # connection.close()
                # find all shards for the server using mapT
                
                for shard in shards:
                    shardList.append(shard)
                    # shard_hash_map[shard].removeServer(server_to_id[serverName])
                    # remove from mapT corresponding to serverId
                    cursor.execute("DELETE FROM mapT WHERE shard_id=%s",(shard))
                deadServerList.append(serverName)
                # del servers_to_shard[serverName]
        for serverName in deadServerList:
            await spawn_server(serverName, shardList)
        await asyncio.sleep(interval)

#  /primary elect, method=GET) - elects a new primary for a shard
