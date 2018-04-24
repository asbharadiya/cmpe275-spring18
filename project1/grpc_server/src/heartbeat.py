# Haoji Liu
# This runs before each node is booted up to register the node to task scheduler
# retrurns True if successfully registered, False otherwise
import requests, time, sys, logging
import socket
import fcntl
import struct

import constants

CONST_TASK_SCHEDULER_HOST = constants.zmq_read_host
CONST_RETRY_SLEEP_INTERVAL = 5 # in seconds
CONST_RETRY_CNT = 99999 # retry 5 times at most

def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    my_ip = socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15].encode())
    )[20:24])
    return my_ip

def try_register(my_ip):
  url = 'http://' + CONST_TASK_SCHEDULER_HOST + '/register/' + my_ip
  status_code = -1
  try:
    r = requests.get(url)
    status_code = r.status_code
  except:
    pass
  return status_code == 200

def ping(my_ip):
  url = 'http://' + CONST_TASK_SCHEDULER_HOST + '/ping/' + my_ip
  r = requests.get(url)
  return r.status_code == 200

is_registered = False
retry_cnt = 0

try:
  my_ip = get_ip_address('eth0').replace('.', '-')  # '192.168.0.110'
except:
  # if you start all nodes at the same time,
  # the ip addr of task scheduler might not be available immediately
  time.sleep(10)
  my_ip = get_ip_address('eth0').replace('.', '-')  # '192.168.0.110'

logging.warning('my ip address is %s' % my_ip)

while True:
  while not is_registered and retry_cnt < CONST_RETRY_CNT:
    logging.warning('trying to register...')
    res = try_register(my_ip)
    if res is True:
      logging.warning('register succeeded...')
      is_registered = True
    else:
      logging.warning('register failed, waiting for retry...')
    time.sleep(CONST_RETRY_SLEEP_INTERVAL)
    retry_cnt = retry_cnt + 1

  if retry_cnt >= CONST_RETRY_CNT:
    logging.warning('register failed, sleeping for 1 hr...')
    # sleep for 1 hour
    time.sleep(3600)
    retry_cnt = 0
  else:
    while True:
      time.sleep(5)
      logging.warning('pinging...')
      try:
        ping(my_ip)
      except:
        retry_cnt = 0
        is_registered = False
        break
