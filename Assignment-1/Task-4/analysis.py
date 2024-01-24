import aiohttp
import asyncio
import matplotlib.pyplot as plt

NUM_REQUESTS = 1000

async def send_request(server_url):
    async with aiohttp.ClientSession() as session:
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
    tasks = [send_request(server_url) for _ in range(num_requests)]
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

def plot_line_chart(data, x_label, y_label, title, path):
    x_values = list(data.keys())
    y_values = list(data.values())
    plt.close()
    plt.plot(x_values, y_values, marker='o')
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.savefig(path)

    
async def main():
    # A-1: Launch 10000 async requests on N = 3 server containers
    server_url = 'http://localhost:5000'
    response_counts_a1 = await send_requests(server_url, NUM_REQUESTS)
    plot_bar_chart(response_counts_a1, 'Experiment A-1: Request Distribution on N=3 Servers', './results/A1.png')

    # A-2: Increment N from 2 to 6 and launch 10000 requests on each increment
    async with aiohttp.ClientSession() as session:
        async with session.delete(server_url + '/rm', json = {"n": 1, "hostnames": []}) as response:
            print(await response.read())
    response_counts_a2 = {}
    server_url = f'http://localhost:5000'
    for n in range(2, 7):
        response_counts = await send_requests(server_url, NUM_REQUESTS)
        average_load = sum(response_counts.values()) / n
        response_counts_a2[n] = average_load
        async with aiohttp.ClientSession() as session:
            async with session.post(server_url + '/add', json = {"n": 1, "hostnames": []}) as response:
                print(await response.read())
    plot_line_chart(response_counts_a2, 'Number of Servers (N)', 'Average Load', 'Experiment A-2: Scalability of Load Balancer', './results/A2.png')

if __name__ == "__main__":
    asyncio.run(main())


# plot_line_chart(response_counts_a2, 'Number of Servers (N)', 'Average Load', 'Experiment A-2: Scalability of Load Balancer')

# A-3: Test all endpoints of the load balancer and simulate server failure
# You need to write code to test all load balancer endpoints and simulate server failure.

# A-4: Modify the hash functions H(i), Φ(i, j) and re-run experiments (A-1) and (A-2)
# Update your load balancer implementation with modified hash functions and rerun experiments.
