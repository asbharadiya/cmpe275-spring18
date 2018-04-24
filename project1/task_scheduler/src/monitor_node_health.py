#!/usr/bin/python
import time, logging
from datetime import datetime, timedelta
# sqlite connection
import sqlite3

import constants, emailer

def report(job):
  # TODO: report error on a given node
  # for now it will just send an email
  logging.warning('going to report the node!!!')

  emailer.send(constants.EMAILER_SENDER,
    constants.EMAILER_SENDER_PASSWD,
    constants.EMAILER_RECIPIENTS,
    constants.EMAILER_NODE_ERROR_UBJECT,
    constants.EMAILER_NODE_ERROR_BODY % job[0])

  return True

def check_health():
  conn = sqlite3.connect('/srv/tmp.db')
  logging.warning('checking nodes health... %s' % str(conn))
  c = conn.cursor()

  for job in c.execute('select * from nodes;'):
    logging.warning(job)
    last_updated_time = datetime.strptime(job[3], '%Y-%m-%d %H:%M:%S.%f')
    diff = datetime.now() - last_updated_time
    logging.warning(diff)
    if diff > timedelta(seconds=constants.MIN_HEARTBEAT_INTERVAL * 2):
      q = "update nodes set flags = 1 where ip_addr == %s" % job[0]
      logging.warning(q)
      c.execute(q)
      logging.warning('find a bad node! %s' % str(job))
      # TODO: use sqlalchemy
      report(job)
      # job.flags = constants.CONST_NODE_FLAG_DEAD
      # conn.add(job)

  conn.commit()
  conn.close()
  return True

while True:
  time.sleep(5)
  check_health()
