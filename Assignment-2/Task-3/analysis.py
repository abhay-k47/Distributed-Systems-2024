import requests
import time
import random
import numpy as np
import matplotlib.pyplot as plt
import os
from payload_generator import PayloadGenerator


base_url = "http://localhost:5000"
generator = PayloadGenerator(24576)


def plot_line_chart(x_values, y_values, x_label, y_label, title, path):
    plt.close()
    if x_values is None:
        plt.plot(y_values)
    else:
        plt.plot(x_values, y_values)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.savefig(path)
    print(f"Saved plot: {title} to {path}")


def launch_rw_requests():
    shuffled_endpoints = ["/read"]*10 + ["/write"]*10
    random.shuffle(shuffled_endpoints)

    read_time = []
    write_time = []
    for endpoint in shuffled_endpoints:
        if endpoint == "/read":
            start = time.time()
            response = requests.post(
                base_url + "/read", json=generator.generate_random_payload(endpoint="/read"))
            if response.status_code != 200:
                print("Error:", response.text)
            read_time.append(time.time() - start)
        elif endpoint == "/write":
            start = time.time()
            response = requests.post(
                base_url + "/write", json=generator.generate_random_payload(endpoint="/write"))
            if response.status_code != 200:
                print("Error:", response.text)
            write_time.append(time.time() - start)

    return read_time, write_time


def send_request(endpoint, method, payload=None):
    url = f"{base_url}/{endpoint}"
    try:
        if method == "GET":
            response = requests.get(url)
        elif method == "POST":
            response = requests.post(url, json=payload)
        elif method == "PUT":
            response = requests.put(url, json=payload)
        elif method == "DELETE":
            response = requests.delete(url, json=payload)
        else:
            return None
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Response: {response.text}")
            print(f"Status Code: {response.status_code}")
    except Exception as e:
        print(f"Request failed: {e}")
    return None


def subtask_a1():

    payload = {
        "N": 3,
        "schema": {
            "columns": [
                "Stud_id",
                "Stud_name",
                "Stud_marks"
            ],
            "dtypes": [
                "Number",
                "String",
                "String"
            ]
        },
        "shards": [
            {
                "Stud_id_low": 0,
                "Shard_id": "sh1",
                "Shard_size": 65536
            },
            {
                "Stud_id_low": 65536,
                "Shard_id": "sh2",
                "Shard_size": 65536
            },
            {
                "Stud_id_low": 131072,
                "Shard_id": "sh3",
                "Shard_size": 65536
            }
        ],
        "servers": {
            "Server0": [
                "sh1",
                "sh2"
            ],
            "Server1": [
                "sh2",
                "sh3"
            ],
            "Server2": [
                "sh1",
                "sh3"
            ]
        }
    }

    if send_request(endpoint="/init", method="POST", payload=payload) is None:
        print("Error in initializing")
        exit(1)

    rtime, wtime = launch_rw_requests()

    print("A-1: Default Configuration")
    print("Total read time:", np.sum(rtime), " seconds")
    print("Total write time:", np.sum(wtime), " seconds")
    print("Average read time:", np.mean(rtime), " seconds")
    print("Average write time:", np.mean(wtime), " seconds")

    plot_line_chart(x_values=None, y_values=rtime, x_label="Request", y_label="Time (s)",
                    title="Read Time", path="A1_read_time.png")
    plot_line_chart(x_values=None, y_values=wtime, x_label="Request", y_label="Time (s)",
                    title="Write Time", path="A1_write_time.png")


def subtask_a2():

    payload = {
        "N": 3,
        "schema": {
            "columns": [
                "Stud_id",
                "Stud_name",
                "Stud_marks"
            ],
            "dtypes": [
                "Number",
                "String",
                "String"
            ]
        },
        "shards": [
            {
                "Stud_id_low": 0,
                "Shard_id": "sh1",
                "Shard_size": 65536
            },
            {
                "Stud_id_low": 65536,
                "Shard_id": "sh2",
                "Shard_size": 65536
            },
            {
                "Stud_id_low": 131072,
                "Shard_id": "sh3",
                "Shard_size": 65536
            }
        ],
        "servers": {
            "Server0": [
                "sh1",
                "sh2"
            ],
            "Server1": [
                "sh2",
                "sh3"
            ],
            "Server2": [
                "sh1",
                "sh3"
            ],
            "Server3": [
                "sh1",
                "sh2"
            ],
            "Server4": [
                "sh2",
                "sh3"
            ],
            "Server5": [
                "sh1",
                "sh3"
            ],
            "Server6": [
                "sh1",
                "sh2"
            ],
            "Server7": [
                "sh2",
                "sh3"
            ],
            "Server8": [
                "sh1",
                "sh3"
            ],
            "Server9": [
                "sh1",
                "sh2",
                "sh3"
            ]
        }
    }

    if send_request(endpoint="/init", method="POST", payload=payload) is None:
        print("Error in initializing")
        exit(1)

    rtime, wtime = launch_rw_requests()

    print("A-2: Increased number of shard replicas")
    print("Total read time:", np.sum(rtime), " seconds")
    print("Total write time:", np.sum(wtime), " seconds")
    print("Average read time:", np.mean(rtime), " seconds")
    print("Average write time:", np.mean(wtime), " seconds")

    plot_line_chart(x_values=None, y_values=rtime, x_label="Request", y_label="Time (s)",
                    title="Read Time", path="A2_read_time.png")
    plot_line_chart(x_values=None, y_values=wtime, x_label="Request", y_label="Time (s)",
                    title="Write Time", path="A2_write_time.png")


def subtask_a3():
    payload = {
        "N": 3,
        "schema": {
            "columns": [
                "Stud_id",
                "Stud_name",
                "Stud_marks"
            ],
            "dtypes": [
                "Number",
                "String",
                "String"
            ]
        },
        "shards": [
            {
                "Stud_id_low": 0,
                "Shard_id": "sh1",
                "Shard_size": 4096
            },
            {
                "Stud_id_low": 4096,
                "Shard_id": "sh2",
                "Shard_size": 4096
            },
            {
                "Stud_id_low": 8192,
                "Shard_id": "sh3",
                "Shard_size": 4096
            },
            {
                "Stud_id_low": 12288,
                "Shard_id": "sh4",
                "Shard_size": 4096
            },
            {
                "Stud_id_low": 16384,
                "Shard_id": "sh5",
                "Shard_size": 4096
            },
            {
                "Stud_id_low": 20480,
                "Shard_id": "sh6",
                "Shard_size": 4096
            }
        ],
        "servers": {
            "Server0": [
                "sh1",
                "sh2",
                "sh3",
                "sh4",
                "sh5",
                "sh6"
            ],
            "Server1": [
                "sh1",
                "sh2",
                "sh3",
                "sh4",
                "sh5",
                "sh6"
            ]
        }
    }

    if send_request(endpoint="/init", method="POST", payload=payload) is None:
        print("Error in initializing")
        exit(1)

    write_times = {}
    read_times = {}

    for n in range(2, 10):
        rtime, wtime = launch_rw_requests()
        write_times[n] = {"total": np.sum(wtime), "mean": np.mean(
            wtime), "error": np.std(wtime)}
        read_times[n] = {"total": np.sum(rtime), "mean": np.mean(
            rtime), "error": np.std(rtime)}
        payload={}
        if n <= 5:
            payload = {
                "n": 1,
                "servers": {
                    f"Server{n}": ["sh1", "sh2", "sh3", "sh4", "sh5", "sh6"]
                }
            }
        elif n<=7:
            payload = {
                "n": 1,
                "servers": {
                    f"Server{n}": ["sh3", "sh4", "sh5", "sh6"]
                }
            }
        else:
            payload={
                "n" : 1,
                "servers":{
                    f"Server{n}": ["sh1", "sh2"]
                }
            }
        if send_request(endpoint="/add", method="POST", payload=payload) is None:
            print("Error in adding server")
            exit(1)

    print("A-3: Varying Number of Servers")
    plot_line_chart(x_values=list(write_times.keys()), y_values=[write_times[n]["mean"] for n in write_times],
                    x_label="Number of Servers", y_label="Time (s)", title="Mean Write Time", path="A3_mean_write_time.png")
    plot_line_chart(x_values=list(write_times.keys()), y_values=[write_times[n]["error"] for n in write_times],
                    x_label="Number of Servers", y_label="Time (s)", title="Error Write Time", path="A3_error_write_time.png")
    plot_line_chart(x_values=list(write_times.keys()), y_values=[write_times[n]["total"] for n in write_times],
                    x_label="Number of Servers", y_label="Time (s)", title="Total Write Time", path="A3_total_write_time.png")
    plot_line_chart(x_values=list(read_times.keys()), y_values=[read_times[n]["mean"] for n in read_times],
                    x_label="Number of Servers", y_label="Time (s)", title="Mean Read Time", path="A3_mean_read_time.png")
    plot_line_chart(x_values=list(read_times.keys()), y_values=[read_times[n]["error"] for n in read_times],
                    x_label="Number of Servers", y_label="Time (s)", title="Error Read Time", path="A3_error_read_time.png")
    plot_line_chart(x_values=list(read_times.keys()), y_values=[read_times[n]["total"] for n in read_times],
                    x_label="Number of Servers", y_label="Time (s)", title="Total Read Time", path="A3_total_read_time.png")

        
if __name__ == "__main__":
    # input to run which subtask
    input = int(input("Enter subtask to run [1/2/3]: "))
    if input == 1:
        subtask_a1()
    elif input == 2:
        subtask_a2()
    elif input == 3:
        subtask_a3()
    else:
        print("Invalid input")
        exit(1)
