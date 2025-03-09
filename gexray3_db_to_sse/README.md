# Gexray3 Project - New GEX api

This uses the new api server to fetch records and serve to the new SSE endpoint. 

[SSE Endpoint](https://sse.sweet-forest-e367.workers.dev/sse/gexray)
SSE Message Type: `{"msg_type": "gex3"}`
Redis Pubsub Channel: `gex2`
Server: `wallstjesus.info`
Script location: `/apps/gexreader/gexray3_db_to_sse`

[Message Viewer](https://wallstjesus.info/sse/gexray_viewer)

# Project Description

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

# Roadmap

- Add ticker list to database queries. 

# Changelog

## v0.0.3

### Added or changed

- Refactored redis connection pooling.
- Refactored script using dataclasses for connection configuration. 
- Added module documentation. 
- Moved some variables to module attributes. 