import json
import redis
from celery import Celery
from config import IPS

broker = f"pyamqp://test:test@{IPS[0]}"
app = Celery("tasks", backend="rpc", broker=broker)
rds = [redis.Redis(host=ip, decode_responses=True, socket_timeout=5) for ip in IPS]

@app.task(acks_late=True, ignore_results=True, bind=True, max_retries=3)
def map(self, filename):
    wc = {}
    with open(filename, mode="r", newline="\r") as f:
        for text in f:
            if text == "\n":
                continue
            sp = text.split(",")[4:-2]
            tweet = " ".join(sp)
            for word in tweet.split(" "):
                if word not in wc:
                    wc[word] = 0
                wc[word] += 1
    member = {filename: json.dumps(wc)}
    writes = []
    while len(writes) < 2:
        for i in range(3):
            if i in writes:
                continue
            try:
                rds[i].sadd("FILESET", json.dumps(member))
                writes.append(i)
            except:
                pass
