# Gexray Project

## Project Description

This project has two current versions. There is the legacy version and the v2 version. 
The legacy version has the old (marketlizards) process and the new (wallstjesus) process. 

The entire legacy process will be deprecated in steps. Marketlizards will be deprecated first. 

## Gexray Legacy

[Readme](private_readme.md)

[SSE Endpoint] [Readme](private_readme.md)
SSE Message Type: `{"msg_type": "gex2"}`  
Redis Pubsub Channel: `gex2`  
[Message Viewer] [Readme](private_readme.md)

Marketlizards process:
1. Celery task reads from API
2. Saves to redis pubsub (marketlizards)
3. Saves to tsdb
4. Serves to secure websocket (marketlizards)

Wallstjesus Process:
1. Reads from tsdb (gexreader2)
2. Serves to redis pubsub (gexreader2)
3. Serves to SSE (wallstjesus)

## Gexray v2

[SSE Endpoint] [Readme](private_readme.md)
SSE Message Type: `{"msg_type": "gex3"}`  
Redis Pubsub Channel: `gex2`  
Server: `wallstjesus`  
Script location: `/apps/gexreader/gexray3_db_to_sse`  
[Message Viewer] [Readme](private_readme.md)

1. Reads from snapshot, processes data and saves to `api` database.  (create_index_naive_gex).  
2. Reads from api database `gex3` and saves to redis pubsub (gexrayreader3).  
3. Serves from pubsub to SSE endpoint (wallstjesus)

## Roadmap

### Gexray Legacy
- Deprecate marketlizards websocket. 
- Deprecate the whole legacy project once Gexray3 is stable.
- Add logic for Holidays to 0DTE expirations. 

# Changelog

## v0.0.3

### Added or changed

- db_to_sse: Refactored redis connection pooling.
- db_to_sse: Refactored script using dataclasses for connection configuration. 
- db_to_sse: Added module documentation. 
- db_to_sse: Moved some variables to module attributes. 
- Moved all related scripts under wallstjesus:gexreader
- Added v2 module to calculate naive gex for stocks and non-0DTE. 
- Added logic for Holidays for weekly expirations. 
- Added dynamic ticker api for chart select query. 


===================================================

# Gexrayreader2 Module - Legacy GEX api

This uses the new api server to fetch records and serve to the new SSE endpoint. 

Script location: `/apps/gexreader/gexray2_db_to_sse`

# How to use the module

- This project is ran with a docker container. 
- The schedule is handled via crontab `/usr/bin/docker start gexrayreader2`

When making changes to the project. 

```
cd /apps/gexreader/gexray2_db_to_sse

docker compose stop gexrayreader2
docker compose build

# Run the container and check the logs for errors.
docker compose up -d
docker logs gexrayreader2 -f

# This will leave the container built and ready to start by cron.
docker stop gexrayreader2

# Make sure the container is ready to run
docker ps -a
```


################################################################################################

# Gexrayreader3 Project - New GEX api

This uses the new api server to fetch records and serve to the new SSE endpoint.

Script location: `/apps/gexreader/gexray3_db_to_sse`

# How to use the module

- This project is ran with a docker container. 
- The schedule is handled via crontab `/usr/bin/docker start gexrayreader3`

When making changes to the project. 

```
cd /apps/gexreader/gexray3_db_to_sse

docker compose stop gexrayreader3
docker compose build

# Run the container and check the logs for errors.
docker compose up -d
docker logs gexrayreader3 -f

# This will leave the container built and ready to start by cron.
docker stop gexrayreader3

# Make sure the container is ready to run
docker ps -a
```

#################################################################################################

# Gexray3 Module

This get the snapshot data for a list of index tickers and calculates gex. 

Script location: `/apps/gexreader/create_index_naive_gex`

# How to use this module 

- This project is ran with a docker container. 
- The schedule is handled by crontab `/usr/bin/docker start gexray3`. 

When making changes to the project. 

```
cd /apps/gexreader/create_index_naive_gex

docker compose stop gexray3
docker compose build

# Run the container and check the logs for errors. 
docker compose up -d
docker logs gexray3 -f

# This will leave the container built and ready to start by cron. 
docker stop gexray3

# Make sure the container is ready to run
docker ps -a
```