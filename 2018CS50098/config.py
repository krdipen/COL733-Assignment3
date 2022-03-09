import os
import subprocess as sp
from time import sleep
from pexpect import pxssh
from myrds import Lab3Redis, ConsistentRedis, AvailableRedis

IPS = ["10.17.50.173", "10.17.6.76", "10.17.50.74"]

def get_redis(consistent: bool) -> Lab3Redis:
  if consistent:
    return ConsistentRedis(IPS)
  return AvailableRedis(IPS)

def get_full_name(ip: str) -> str:
  return f"student@{ip}"

def setup_rabbit(ip: str) -> None:

  print(f"\n\t=> Starting rabbitmq on {ip}\n")
  s = pxssh.pxssh()
  s.login(ip, "rabbitmq", "rabbitmq")
  s.prompt()

  s.sendline("rabbitmqctl stop_app")
  s.prompt()
  print(s.before)
  print("Rabbitmq Stopped")

  sleep(5)  # wait for rabbitmq to stop

  s.sendline("rabbitmqctl start_app")
  s.prompt()
  print(s.before)
  print("Rabbitmq Started")

  s.sendline("rabbitmqctl list_users")
  s.prompt()
  print(s.before)
  print("Listed Rabbitmq users list")
  
  list_users = (s.before).decode()
  if "test" not in list_users:

    s.sendline("rabbitmqctl add_user test test")
    s.prompt()
    print(s.before)
    print("Added Test User")

    s.sendline("rabbitmqctl set_permissions -p / test '.*' '.*' '.*'")
    s.prompt()
    print(s.before)
    print("Permissions are set")

  s.logout()

def purge_celery(ip: str) -> None:

  print(f"\n\t=> Deleting celery workers on {ip}\n")
  s = pxssh.pxssh()
  s.login(ip, "student", "pass123") # update the password
  s.prompt()

  s.sendline("kill -9 $(ps -ef | grep celery | awk '{print $2}')")
  s.prompt()
  print(s.before)
  print("Celery Tasks deleted")

  s.sendline("celery -A tasks purge -f")
  s.prompt()
  print(s.before)
  print("Purge celery task")

  s.logout()

def setup_rds(ip: str, consistent: bool) -> None:

  print(f"\n\t=> Setup consistent={consistent} redis on {ip}\n")
  s = pxssh.pxssh()
  s.login(ip, "student", "pass123") # update the password
  s.prompt()

  s.sendline("fuser -k 6379/tcp")
  s.prompt()
  print(s.before)
  print("Redis ports freed")

  s.sendline("rm -rf dump.rdb")
  s.prompt()
  print(s.before)
  print("Redis dump deleted")

  s.sendline("rm -rf nohup.out")
  s.prompt()
  print(s.before)
  print("Redis old output cleared")

  s.sendline(f"redis-cli -h {ip} SHUTDOWN")
  s.prompt()
  print(s.before)
  print("Redis shutdown")

  if consistent:
    s.sendline("redis-server redis.conf --loadmodule ~/redisraft/redisraft.so")
    s.prompt()
    print(s.before)
    print("Raft cluster initialized")
    if ip == IPS[0]:
      s.sendline(f"redis-cli -h {ip} RAFT.CLUSTER INIT")
      s.prompt()
      print(s.before)
      print("Redis started")
    else:
      s.sendline(f"redis-cli -h {ip} RAFT.CLUSTER JOIN {IPS[0]}")
      s.prompt()
      print(s.before)
      print("Redis started")
  else:
    s.sendline("nohup redis-server redis.conf&")
    s.prompt()
    print(s.before)
    print("Redis started")

  s.logout()

def copy_code(ip: str) -> None:

  print(f"\n\t=> Copying code to {ip}\n")
  full_name = get_full_name(ip)
  s = pxssh.pxssh()
  s.login(ip, "student", "pass123") # update the password
  s.prompt()

  print("Creating lab3 folder")
  sp.run(["ssh", full_name, "mkdir -p ~/labs/lab3/"]).check_returncode()
  sp.run(["ssh", full_name, "rm -rf ~/labs/lab3/*"]).check_returncode()

  print("Copying code")
  mydir = os.path.dirname(os.path.realpath(__file__))
  sp.run(["scp", "-r", mydir, f"{full_name}:~/labs/lab3/"]).check_returncode()

  s.sendline("mv ./labs/lab3/**/* ./labs/lab3/")
  s.prompt()
  print(s.before)
  print("Inside the lab3 folder")

  s.logout()

def setup_celery(ip: str, worker_name: str) -> None:

  print(f"\n\t=> Setup celery workers on {ip}\n")
  s = pxssh.pxssh()
  s.login(ip, "student", "pass123") # update the password
  s.prompt()

  s.sendline("cd ~/labs/lab3/")
  s.prompt()
  print(s.before)
  print("Inside the lab3 folder")

  s.sendline(f"nohup celery -A tasks worker --loglevel=INFO --concurrency=8 -n {worker_name}@%h&")
  s.prompt()
  print(s.before)
  print("Celery Worker started")

  s.logout()

if __name__ == "__main__":

  CONSISTENT = False
  print(f"Configuring for consistent = {CONSISTENT}")

  setup_rabbit(IPS[0])

  for ip in IPS:
    purge_celery(ip)
    
  for ip in IPS:
    setup_rds(ip, CONSISTENT)
    copy_code(ip)

  for i, ip in enumerate(IPS):
    worker_name = f"worker{i}"
    setup_celery(ip, worker_name)
