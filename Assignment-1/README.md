# Assignment 1

## Initial Setup

- Install Docker using the [script](./docker_install.sh)

## Task-1: Server

- Change working directory to `Assignment-1/Task-1`
- Build the docker image using the command `docker build -t task1-image .`
- Run the docker container using the command `docker run -it -p <port>:5000 -e SERVER_ID=<server-id> task1-image`
- The server is now running on the specified port

## Task - 2: Consistent Hashing

- Implemented Consistent Hashing class using an array, with linear/quadratic probing and default arguments as
    - Number of Server Containers managed by the load balancer (N) = 3
    - Total number of slots in the consistent hash map (#slots) = 512
    - Number of virtual servers for each server container (K) = $log$(512) = 9
    - Hash function for request mapping H($i$) = $i^2 + 2i + 17$
    - Hash function for virtual server mapping Φ($i$, $j$) = $i^2 + j^2 + 2j + 25$