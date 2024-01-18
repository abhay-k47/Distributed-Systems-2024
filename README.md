# Distributed-Systems-2024

## Initial Setup

- Install Docker using the [script](./docker_install.sh)

## Task-1: Server

- Change working directory to `Assignment-1/Task-1`
- Build the docker image using the command `docker build -t task1-image .`
- Run the docker container using the command `docker run -it -p <port>:5000 -e SERVER_ID=<server-id> task1-image`
- The server is now running on the specified port
