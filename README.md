sudo chmod u+w /apps/gexreader/fake.log

nohup uv run main.py > /apps/gexreader/gexreader.log 2>&1 &

ps aux | grep "uv run main.py"

kill 12345

######### Fake script

ps aux | grep "uv run fake.py"


############# Gex Poster 3.0

nohup uv run gexray3_main.py > /apps/gexreader/gexreader3.log 2>&1 &

## Gexray

These scripts load the data from the database for the current day and send to the SSE channel.
This is the original gexray that pulls data from Gexbot. 

The service that starts Gexray is gexray.service
The service that stop it is gexray-stop.service
The timers are gexray-stop.timer and gexray.timer


## Gexray3

These scripts load the data from the database for the current day and send to the SSE channel. 
This is the new data. 

The service that starts Gexray3 is gexray3.service
The service that stop it is gexray3-stop.service
The timers are gexray3-stop.timer and gexray3.timer