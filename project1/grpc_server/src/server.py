# Haoji Liu
import time, logging
import grpc
import data_pb2
import data_pb2_grpc

from concurrent import futures

import data_server

import zmq

def run(host, port):
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
  ds = data_server.DataServer()
  data_pb2_grpc.add_CommunicationServiceServicer_to_server(ds, server)
  server.add_insecure_port('%s:%d' % (host, port))
  server.start()

  _ONE_DAY_IN_SECONDS = 60 * 60 * 24
  try:
    logging.warning("Server started at...%d" % port)
    while True:
      time.sleep(_ONE_DAY_IN_SECONDS)
  except KeyboardInterrupt:
    server.stop(0)


if __name__ == '__main__':
  run('0.0.0.0', 8080)
