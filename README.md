# Gexray Project

## Project Description

This project has two current versions. There is the legacy version and the v2 version. 
The legacy version has the old (marketlizards) process and the new (wallstjesus) process. 

The entire legacy process will be deprecated in steps. Marketlizards will be deprecated first. 

## Roadmap

### Gexray Legacy
- Deprecate marketlizards.com websocket. 
- Deprecate this whole project once Gexray3 is stable.

### Gexray V2

- Add ticker list to database queries. 

## Current Project Structure

### Gexray Legacy

Marketlizards process:
1. Celery task reads from API
2. Saves to redis pubsub (marketlizards.com)
3. Saves to tsdb
4. Serves to secure websocket (marketlizards.com)

Wallstjesus Process:
1. Reads from tsdb (gexreader2)
2. Serves to redis pubsub (gexreader2)
3. Serves to SSE (wallstjesus.info)

### Gexray v2

# Changelog

## v0.0.3

### Added or changed

- db_to_sse: Refactored redis connection pooling.
- db_to_sse: Refactored script using dataclasses for connection configuration. 
- db_to_sse: Added module documentation. 
- db_to_sse: Moved some variables to module attributes. 


===================================================

# Gexray2 Module - Legacy GEX api

This uses the new api server to fetch records and serve to the new SSE endpoint. 

[SSE Endpoint](https://sse.sweet-forest-e367.workers.dev/sse/gexray)
SSE Message Type: `{"msg_type": "gex2"}`
Redis Pubsub Channel: `gex2`
Server: `wallstjesus.info`
Script location: `/apps/gexreader/gexray2_db_to_sse`

[Message Viewer](https://wallstjesus.info/sse/gexray_viewer)

# How to use the project

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

# Gexray3 Project - New GEX api

This uses the new api server to fetch records and serve to the new SSE endpoint. 

[SSE Endpoint](https://sse.sweet-forest-e367.workers.dev/sse/gexray)
SSE Message Type: `{"msg_type": "gex3"}`  
Redis Pubsub Channel: `gex2`  
Server: `wallstjesus.info`  
Script location: `/apps/gexreader/gexray3_db_to_sse`  

[Message Viewer](https://wallstjesus.info/sse/gexray_viewer)

# Project Description

1. Reads from snapshot, processes data and saves to `api` database.  (options_flow_server).  
2. 

# How to use the project

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
