# Assignment 2

## Initial Setup

- Install Docker using the [script](../Assignment-1/docker_install.sh)

## Task-1: Server

- Change working directory to `Assignment-2/Task-1`
- Build the docker image using the command `docker build -t server-image .`
- Run the docker container using the command `docker run -it -p <port>:5000 server-image`
- The server is now running on the specified port
