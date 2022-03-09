import os
import sys
from celery import group
from time import sleep
import subprocess
from myrds import current_ip
from config import get_redis, IPS
from tasks import map

if __name__ != "__main__":
  sys.exit(1)

CONSISTENT = False
print(f"Running with CONSISTENT = {CONSISTENT}")

if (len(sys.argv) < 2):
  print("Use the command: python3 client.py <data_dir>")

DIR=sys.argv[1]

faulty_node_ips = list(set(IPS).difference({current_ip}))

isolate_command = \
  f'''sudo iptables -I INPUT 1 -s {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP; 
  sudo iptables -I OUTPUT 1 -d {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP;'''

heal_command = \
  f'''sudo iptables -D INPUT -s {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP; 
  sudo iptables -D OUTPUT -d {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP;'''

abs_files=[os.path.join(pth, f) for pth, _, files in os.walk(DIR) for f in files]

subprocess.run([isolate_command], shell=True, text=True, input='pass123\n')
print("The network partition is in place")

job = group(map.s(files) for files in abs_files)
print(job)
results=job.apply_async()
print(results.get())
rds = get_redis(CONSISTENT)

wc = rds.get_top_words(10)
print(wc)

subprocess.run([heal_command], shell=True, text=True, input='pass123\n')
print("The network partition is healed")

wc = rds.get_top_words(10, True)
print(wc)
