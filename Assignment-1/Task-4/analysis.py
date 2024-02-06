import aiohttp
import asyncio
import matplotlib.pyplot as plt
import numpy as np
import os

timeout = aiohttp.ClientTimeout(total=900)

NUM_REQUESTS = 10000

async def send_request(session, server_url):
    async with session.get(server_url + '/home') as response:
        try:
            if response.status == 200:
                message = await response.json()
                server_id = message.get('message', '').split(':')[-1].strip()
                return server_id
            else:
                print(f"Unexpected status code: {response.status}")
        except Exception as e:
            print(f"Request failed: {e}")
    return -1

async def send_requests(server_url, num_requests):
    response_counts = {}

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [send_request(session, server_url) for _ in range(num_requests)]
        responses = await asyncio.gather(*tasks)

    for server_id in responses:
        if response_counts.get(server_id) == None:
            response_counts[server_id] = 0
        response_counts[server_id] += 1
    return response_counts

def plot_bar_chart(response_counts, title, path):
    servers = list(response_counts.keys())
    counts = list(response_counts.values())
    plt.close()
    plt.bar(servers, counts)
    plt.title(title)
    plt.xlabel('Server ID')
    plt.ylabel('Request Count')
    plt.savefig(path)
    print(f"Saved plot: {title} to {path}")

def plot_line_chart(data, x_label, y_label, title, path):
    x_values = list(data.keys())
    y_values = list(data.values())
    plt.close()
    plt.plot(x_values, y_values, marker='o')
    for x_value,y_value in zip(x_values,y_values):
        plt.text(x_value, y_value, f'{y_value}', ha='left', va='bottom')
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.savefig(path)
    print(f"Saved plot: {title} to {path}")

async def test_endpoint(server_url, endpoint, method='GET', payload=None, log=True):
    url = f"{server_url}/{endpoint}"
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            if method == 'GET':
                async with session.get(url) as response:
                    await handle_response(response)
            elif method == 'POST':
                async with session.post(url, json=payload) as response:
                    await handle_response(response)
            elif method == 'DELETE':
                async with session.delete(url,json=payload) as response:
                    await handle_response(response)
    except Exception as e:
        print(f"Request failed: {e}")

async def handle_response(response):
    print(f"Status Code: {response.status}")
    try:
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            json_data = await response.json()
            print(f"Response JSON: {json_data}")
        else:
            text_data = await response.text()
            print(f"Response Content: {text_data}")
    except Exception as e:
        print(f"Error handling response: {e}")

async def test_endpoints(server_url):

    print("Testing /rep endpoint")
    await test_endpoint(server_url, 'rep')

    print("Testing /home endpoint")
    await test_endpoint(server_url, 'home')

    print("Testing /heartbeat endpoint")
    await test_endpoint(server_url, 'heartbeat')

    print("Testing /add endpoint")
    payload = {"n": 1, "hostnames": ["S1"]}
    print(f"Payload: {payload}")
    await test_endpoint(server_url, 'add', method='POST', payload=payload)
    payload = {"n": 2, "hostnames": ["S2"]}
    print(f"Payload: {payload}")
    await test_endpoint(server_url, 'add', method='POST', payload=payload)
    payload = {"n": 2, "hostnames": ["S3", "S4", "S5"]}
    print(f"Payload: {payload}")
    await test_endpoint(server_url, 'add', method='POST', payload=payload)

    print("Testing /rm endpoint")
    payload = {"n": 1, "hostnames": ["S1"]}
    print(f"Payload: {payload}")
    await test_endpoint(server_url, 'rm', method='DELETE', payload=payload)
    payload = {"n": 2, "hostnames": ["S2"]}
    print(f"Payload: {payload}")
    await test_endpoint(server_url, 'rm', method='DELETE', payload=payload)
    payload = {"n": 2, "hostnames": ["S3", "S4", "S5"]}
    print(f"Payload: {payload}")
    await test_endpoint(server_url, 'rm', method='DELETE', payload=payload)
    payload = {"n": 2, "hostnames": ["S6"]}
    print(f"Payload: {payload}")
    await test_endpoint(server_url, 'rm', method='DELETE', payload=payload)

async def simulate_server_failure(server_url,serverName):
    print(f"Removing server with hostname={serverName}")
    try:
        os.system(f"sudo docker stop {serverName} && sudo docker rm {serverName}")
        print(f"Server with hostname={serverName} removed successfully")
        await asyncio.sleep(5)
    except Exception as e:
        print(f"Request failed: {e}")
    response_counts = await send_requests(server_url, NUM_REQUESTS)
    print(response_counts)
    plot_bar_chart(response_counts, 'Experiment A-3: Request Distribution after Server Failure', './results/A3.png')

    
async def main():
    server_url = 'http://localhost:5000'

    # # A-1: Launch 10000 async requests on N = 3 server containers
    print("Launching 10000 async requests on N = 3 server containers")
    response_counts_a1 = await send_requests(server_url, NUM_REQUESTS)
    plot_bar_chart(response_counts_a1, 'Experiment A-1: Request Distribution on N=3 Servers', './results/A1.png')

    # # A-2: Increment N from 2 to 6 and launch 10000 requests on each increment
    print("Incrementing N from 2 to 6 and launching 10000 requests on each increment")
    await test_endpoint(server_url, 'rm', method='DELETE', payload={"n": 1, "hostnames": []})
    load_mean = {}
    load_error = {}
    for n in range(2, 7):
        response_counts = await send_requests(server_url, NUM_REQUESTS)
        load_mean[n] = np.mean(list(response_counts.values()))
        load_error[n] = np.std(list(response_counts.values()))
        await test_endpoint(server_url, 'add', method='POST', payload={"n": 1, "hostnames": []})
    plot_line_chart(load_mean, 'Number of Servers (N)', 'Average Load', 'Experiment A-2: Scalability of Load Balancer', './results/A2-mean.png')
    plot_line_chart(load_error, 'Number of Servers (N)', 'Standard Deviation of Load on Servers', 'Experiment A-2: Scalability of Load Balancer', './results/A2-err.png')


    # A-3: Test all endpoints of the load balancer and simulate server failure
    print("Testing all endpoints of the load balancer")
    await test_endpoints(server_url)

if __name__ == "__main__":
    asyncio.run(main())
