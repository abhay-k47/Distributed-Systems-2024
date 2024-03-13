FROM mysql:8.0-debian

COPY deploy.sh /always-initdb.d/
COPY . /app
WORKDIR /app

RUN apt-get update && \
    apt-get install -y python3 && \
    apt-get install -y python3-pip && \
    apt-get install -y python3-venv && \
    python3 -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

RUN chmod +x /app/custom-entry.sh

ENV MYSQL_ROOT_PASSWORD="abc"

EXPOSE 5000

ENTRYPOINT ["/app/custom-entry.sh"]
CMD ["mysqld"]