FROM python:3.8-slim

WORKDIR /app

COPY . /app

RUN apt-get update \
    && apt-get -y install sudo \
    && apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - \
    && add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" \
    && apt-get update \
    && apt-get -y install docker.io

ENV USER=theuser
RUN adduser --home /home/$USER --disabled-password --gecos GECOS $USER \
    && echo "$USER ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/$USER \
    && chmod 0440 /etc/sudoers.d/$USER \
    && usermod -aG docker $USER \
    && chsh -s /bin/zsh $USER
USER $USER

ENV HOME=/home/$USER

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD [ "python", "load_balancer.py" ]