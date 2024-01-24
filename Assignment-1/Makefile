build:
	sudo docker-compose build

up:
	sudo docker-compose up

down:
	sudo docker-compose down

clean:
	sudo docker ps -aq | xargs sudo docker rm -vf
	sudo docker images -aq| xargs sudo docker rmi -f
