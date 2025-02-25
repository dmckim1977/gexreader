sudo chmod u+w /apps/gexreader/fake.log

nohup uv run main.py > /apps/gexreader/gexreader.log 2>&1 &

ps aux | grep "uv run main.py"

kill 12345

######### Fake script

ps aux | grep "uv run fake.py"