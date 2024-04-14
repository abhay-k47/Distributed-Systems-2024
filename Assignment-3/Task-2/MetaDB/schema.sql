CREATE DATABASE IF NOT EXISTS MetaDB;
USE MetaDB;
CREATE TABLE IF NOT EXISTS ShardT(
    Stud_id_low INT PRIMARY KEY,
    Shard_id INT,
    Shard_size INT
);
CREATE TABLE IF NOT EXISTS MapT(
    Shard_id INT,
    Server_id INT,
    Is_primary BOOLEAN
);
CREATE TABLE IF NOT EXISTS ServerT(
    Server_id INT PRIMARY KEY,
    Server_name VARCHAR(64)
);