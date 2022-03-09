from abc import ABC
from typing import Tuple, List
import redis
import json

current_ip = "10.17.50.173"

class Lab3Redis(ABC):

  def __init__(self, ips: List[str]):
    self.conns = [redis.Redis(host=ip, decode_responses=True, socket_timeout=5) for ip in ips]
    self.num_instances = len(ips)
    self.index = ips.index(current_ip)

  def get_top_words(self, n: int, repair: bool = False) -> List[Tuple[str, int]]:
    pass

class ConsistentRedis(Lab3Redis):

  def get_top_words(self, n: int, repair: bool = False) -> List[Tuple[str, int]]:
    pass

class AvailableRedis(Lab3Redis):

  def get_top_words(self, n: int, repair: bool = False) -> List[Tuple[str, int]]:

    if repair:
      sets = [rds.smembers("FILESET") for rds in self.conns]
      for i in range(len(sets)):
        for j in range(len(sets)):
          diff = sets[i] - sets[j]
          if len(diff) == 0:
            continue
          self.conns[j].sadd("FILESET", *diff)

    members = self.conns[self.index].smembers("FILESET")
    wc = {}
    for member in members:
      for word, count in json.loads(list(json.loads(member).values())[0]).items():
        if word not in wc:
          wc[word] = 0
        wc[word] += count
    return sorted(wc.items(), key=lambda x: (x[1],x[0]), reverse=True)[0:n]
