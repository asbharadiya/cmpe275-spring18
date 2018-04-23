# Haoji Liu
import datetime
import grpc_client

"""API v1.0"""
import requests

CONST_TIMESTAMP_FMT = '%Y-%m-%d %H:%M:%S'

nodes = requests.get('https://cmpe275-spring-18.mybluemix.net/get').text.split(',')
print('getting nodes list %s' % nodes)

def api_get(fpath, from_utc, to_utc, host, port, sender, params_json=None):
  """
  params_json - str, a json string, list of dicts
  """
  client = grpc_client.Client(host, port, sender)
  try:
    datetime.datetime.strptime(from_utc, CONST_TIMESTAMP_FMT)
    datetime.datetime.strptime(to_utc, CONST_TIMESTAMP_FMT)
  except:
    return False

  with open(fpath, 'w') as fp:
    if not client.get(fp, from_utc=from_utc, to_utc=to_utc, params_json=params_json):
      for node in nodes:
        print('trying... %s' % node)
        if node == host:
          continue
        client = grpc_client.Client(node, port, host)
        if client.get(fp, from_utc=from_utc, to_utc=to_utc, params_json=params_json):
          print('get succeeded at one of the other nodes')
          break
      print('get failed at all other nodes')
      return False
    else:
      print('get succeeded at this node')
  return True

def api_put(fp, is_broadcast, host, port, sender):
  client = grpc_client.Client(host, port, sender)

  if not is_broadcast:
    res = client.put(fpath=fp)
  if is_broadcast or not res:
    print('going to broadcast...')
    for node in nodes:
      if node == host:
        continue
      print('going to put data to node %s' % node)
      try:
        client = grpc_client.Client(node, port, host)
        if client.put(fpath=fp):
          print('put succeeded at %s' % node)
      except Exception as e:
        print(e)
        print('put failed at node %s' % node)
        return False

  return True

def api_ping(message, host, port, sender):
  client = grpc_client.Client(host, port, sender)
  client.ping(msg=message)
