
from quart import Quart, jsonify, Response, request
import asyncio
import aiohttp
import os
import time
import logging
import mysql.connector
from mysql.connector import Error


class SQLHandler:
    def __init__(self):
        self.mydb = None

    def query(self, sql, value=None):
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
        self.mydb.commit()
        cursor = self.mydb.cursor()
        try:
            cursor.execute(sql, value) if value else cursor.execute(sql)
        except Error as e:
            logging.error(f'Error while executing {sql}, got exception {e}')
        res = cursor.fetchall()
        cursor.close()
        self.mydb.commit()
        app.logger.debug(f"Query: {sql} returned {res}")
        return res

    def close(self):
        if self.mydb is not None:
            self.mydb.close()


app = Quart(__name__)
logging.basicConfig(level=logging.DEBUG)
PORT = 5000
sql = SQLHandler()

# configs server for particular schema and shards


async def config_server(serverName, schema, shards):
    app.logger.info(f"Configuring {serverName}")
    while not await check_heartbeat(serverName, log=False):
        await asyncio.sleep(2)
    async with aiohttp.ClientSession() as session:
        payload = {"schema": schema, "shards": shards}
        async with session.post(f'http://{serverName}:5000/config', json=payload) as resp:
            return resp.status == 200


# dead server restores shards from other servers
async def restore_shards(serverName, shards):
    for shard in shards:
        shard_data = await get_shard_data(shard)
        await write_shard_data(serverName, shard, shard_data)
        shard_WAL = await get_shard_WAL(shard)
        await write_shard_WAL(serverName, shard, shard_WAL)


# gets the shard data from available server
async def get_shard_data(shard):
    print(f"Getting shard data for {shard} from primary server")
    serverName = sql.query(
        f"SELECT s.Server_name FROM MapT m JOIN ServerT s ON m.Server_id=s.Server_id WHERE m.Shard_id='{shard}' AND m.Is_primary = true")[0][0]
    async with aiohttp.ClientSession() as session:
        payload = {"shards": [shard]}
        async with session.get(f'http://{serverName}:5000/copy', json=payload) as resp:
            result = await resp.json()
            data = result.get(shard, None)
            return data if resp.status == 200 else None


# writes the shard data into server
async def write_shard_data(serverName, shard, data):
    res = sql.query(
        f"SELECT s.Server_name FROM MapT m JOIN ServerT s ON m.Server_id=s.Server_id WHERE m.Shard_id='{shard}' AND m.Is_primary = false")
    sec_servers = [r[0] for r in res]
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "data": data, "sec_servers": sec_servers}
        async with session.post(f'http://{serverName}:5000/write', json=payload) as resp:
            return resp.status == 200


async def get_shard_WAL(shard):
    print(f"Getting shard WAL for {shard} from primary server")
    serverName = sql.query(
        f"SELECT s.Server_name FROM MapT m JOIN ServerT s ON m.Server_id=s.Server_id WHERE m.Shard_id='{shard}' AND m.Is_primary = true")[0][0]
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard}
        async with session.get(f'http://{serverName}:5000/get_wal', json=payload) as resp:
            result = await resp.json()
            wal = result.get("WAL", None)
            return wal if resp.status == 200 else None


# writes the shard data into server
async def write_shard_WAL(serverName, shard, WAL):
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "WAL": WAL}
        async with session.post(f'http://{serverName}:5000/set_wal', json=payload) as resp:
            return resp.status == 200

# first spawns server, configures it, restores shards, then updates the required maps


async def respawn_server(serverName, shards, schema={"columns": ["Stud_id", "Stud_name", "Stud_marks"], "dtypes": ["Number", "String", "Number"]}):

    serverId = sql.query(
        f'SELECT Server_id from ServerT WHERE Server_name="{serverName}"')[0][0]
    containerName = serverName
    res = os.popen(
        f"docker run --name {containerName} --network net1 --network-alias {containerName} -e SERVER_NAME={containerName} -d server").read()
    if res == "":
        app.logger.error(f"Error while spawning {containerName}")
        return
    else:
        app.logger.info(f"Spawned {containerName}")
        try:
            await config_server(serverName, schema, shards)
            app.logger.info(f"Configured {containerName}")
            for shard in shards:
                primary = sql.query(
                    f"SELECT s.Server_name FROM MapT m JOIN ServerT s ON m.Server_id=s.Server_id WHERE m.Shard_id='{shard}' AND m.Is_primary = true")[0][0]
                if primary == serverName:
                    res = sql.query(
                        f"SELECT s.Server_name FROM MapT m JOIN ServerT s ON m.Server_id=s.Server_id WHERE m.Shard_id='{shard}' AND m.Is_primary = false")
                    sec_servers = [r[0] for r in res]
                    new_primary = await elect_primary(shard, sec_servers)
                    primary_id = sql.query(
                        f"SELECT Server_id FROM ServerT WHERE Server_name='{new_primary}'")[0][0]
                    sql.query(
                        f"UPDATE MapT SET Is_primary=false WHERE Shard_id='{shard}' AND Server_id={serverId}")
                    sql.query(
                        f"UPDATE MapT SET Is_primary=true WHERE Shard_id='{shard}' AND Server_id={primary_id}")
            await restore_shards(serverName, shards)
            app.logger.info(f"Restored shards for {containerName}")
        except Exception as e:
            app.logger.error(
                f"Error while spawning {containerName}, got exception {e}")


# checks periodic heartbeat of server
async def check_heartbeat(serverName, log=True):
    try:
        if (log):
            app.logger.info(f"Checking heartbeat of {serverName}")
        async with aiohttp.ClientSession(trust_env=True) as client_session:
            async with client_session.get(f'http://{serverName}:5000/heartbeat') as resp:
                return resp.status == 200
    except Exception as e:
        if log:
            app.logger.error(
                f"Error while checking heartbeat of {serverName}: {e}")
        return False


async def periodic_heatbeat_check(interval=3):
    app.logger.info("Starting periodic heartbeat check")
    while True:
        res = sql.query("SELECT Server_name FROM ServerT")
        servers = [r[0] for r in res]
        deadServerList = []
        tasks = [check_heartbeat(serverName) for serverName in servers]
        results = await asyncio.gather(*tasks)
        results = zip(servers, results)
        for serverName, isUp in results:
            if isUp == False:
                app.logger.error(f"Server {serverName} is down")
                deadServerList.append(serverName)
        for serverName in deadServerList:
            serverId = sql.query(
                f'SELECT Server_id from ServerT WHERE Server_name="{serverName}"')[0][0]
            res = sql.query(
                f'SELECT Shard_id from MapT WHERE Server_id={serverId}')
            shardList = [r[0] for r in res]
            await respawn_server(serverName, shardList)
        await asyncio.sleep(interval)


async def elect_primary(shard, sec_servers):
    highest_seq_no = -1
    new_primary = None
    for serverName in sec_servers:
        async with aiohttp.ClientSession() as session:
            payload = {"shard": shard}
            async with session.get(f'http://{serverName}:5000/get_seq', json=payload) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    seq = result['seq']
                    if (seq > highest_seq_no):
                        highest_seq_no = seq
                        new_primary = serverName
    return new_primary


@app.route('/primary_elect', methods=['PUT'])
async def primary_elect():
    payload = await request.get_json()
    servername = payload.get('server', None)
    if servername is None:
        return Response(status=400, response="Server name not provided")
    try:
        serverId = sql.query(
            f'SELECT Server_id from ServerT WHERE Server_name="{servername}"')[0][0]
        res = sql.query(
            f'SELECT Shard_id from MapT WHERE Server_id={serverId} AND Is_primary=true')
        shards = [r[0] for r in res]
        for shard in shards:
            res = sql.query(
                f'SELECT s.Server_name FROM MapT m JOIN ServerT s ON m.Server_id=s.Server_id WHERE m.Shard_id="{shard}" AND m.Is_primary = false')
            sec_servers = [r[0] for r in res]
            new_primary = await elect_primary(shard, sec_servers)
            primary_id = sql.query(
                f'SELECT Server_id FROM ServerT WHERE Server_name="{new_primary}"')[0][0]
            sql.query(
                f'UPDATE MapT SET Is_primary=false WHERE Shard_id="{shard}" AND Server_id={serverId}')
            sql.query(
                f'UPDATE MapT SET Is_primary=true WHERE Shard_id="{shard}" AND Server_id={primary_id}')
    except Exception as e:
        app.logger.error(
            f"Error while electing primary for {servername}, got exception {e}")
        return Response(status=500)
    return Response(status=200)


@app.before_serving
async def startup():
    app.logger.info("Starting the Shard Manager")
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_heatbeat_check())


@app.after_serving
def shutdown():
    sql.close()
    app.logger.info("Shutting down the Shard Manager")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=False)
