import time, datetime
import grpc
import data_pb2
import data_pb2_grpc

import json
import os, logging, sys

import zmq
import constants, util

zmq_context = zmq.Context()

write_host = util.try_get_ip(constants.zmq_write_host)
read_host = util.try_get_ip(constants.zmq_read_host)

write_connect_string = 'tcp://{}:{}'.format(
    write_host, constants.write_port)

read_connect_string = 'tcp://{}:{}'.format(
    read_host, constants.read_client_port)

CONST_TIMESTAMP_FMT = '%Y-%m-%d %H:%M:%S'

def get_write_socket():
  write_sock = zmq_context.socket(zmq.PUB)

  write_sock.bind(write_connect_string)
  # TODO: try not rebind with every grpc call, let's do a timeout of 4 hour or something
  time.sleep(1) # this is needed so that we talk after the bind is complete!
  logging.warning('write socket connected to %s' % write_connect_string)

  return write_sock

def get_read_socket():
  # Open a new socket per request
  read_client_sock = zmq_context.socket(zmq.REQ)
  read_client_sock.setsockopt(zmq.LINGER, 100)
  # 10 sec read timeout
  read_client_sock.setsockopt(zmq.RCVTIMEO, 1000)

  logging.warning('read socket connecting to %s' % read_connect_string)
  read_client_sock.connect(read_connect_string)
  return read_client_sock

def read(sock, params):
  # TODO: add more params like table name, target station
  logging.warning('trying to send a req to read socket...')
  logging.warning(sock)
  sock.send_json(params)
  # wait for response
  logging.warning('waiting for read response...')
  resp = sock.recv_multipart()
  return resp

def try_read(params):
  read_client_sock = None
  try:
    read_client_sock = get_read_socket()
    resp = read(read_client_sock, params)
    logging.warning(resp)
    for r in resp:
      yield r

  except Exception as e:
    logging.warning(e)
    return {'msg': 'something wrong with read...'}
  finally:
    # Make sure it's closed
    if read_client_sock:
      # logging.warning('closing the read socket...')
      read_client_sock.disconnect(read_connect_string)
      read_client_sock.close()

### TODO: should we have a separate peek socket????
def pre_read_check(params):
  # TODO: if we don't have it, send query to other clusters
  # Check the validity of timestamps
  try:
    datetime.datetime.strptime(params['from_utc'], CONST_TIMESTAMP_FMT)
    datetime.datetime.strptime(params['to_utc'], CONST_TIMESTAMP_FMT)
  except Exception as e:
    logging.warning(e)
    return False
  return True

def pre_write_check():
  # TODO: if we can't handle the data, send data to other clusters
  for resp in try_read({'pre_write_check': True}):
    logging.warning(resp)
    if str(resp) == 'True':
      logging.warning('disk full!!!!')
      return False
  return True

def write(payload):
  write_sock = None
  try:
    write_sock = get_write_socket()
    write_sock.send_json(payload)
  except Exception as e:
    logging.warning('something wrong with write...')
    logging.warning(e)
  finally:
    # Make sure it's closed
    if write_sock:
      logging.warning('closing the write socket...')
      write_sock.disconnect(write_connect_string)
      write_sock.close()

  return True

class DataServer(data_pb2_grpc.CommunicationServiceServicer):
  # def __init__(self, read_sock, write_sock):
  #   self.read_sock = read_sock
  #   self.write_sock = write_sock
  def putHandler(self, request_iterator, context):
    logging.warning('this is a put request')
    if pre_write_check():
      for request in request_iterator:
        assert request.putRequest.metaData.uuid is not None
        payload = {
          'raw': request.putRequest.datFragment.data.decode(),
          'uuid': request.putRequest.metaData.uuid}
        # timestamp for mesonet only, mesowest each row has diff timestamps
        if request.putRequest.datFragment.timestamp_utc:
          logging.warning('mesonet!')
          payload['timestamp_utc'] = request.putRequest.datFragment.timestamp_utc
        else:
          logging.warning('mesowest!')
        write(payload)
      return data_pb2.Response(
        code=data_pb2.StatusCode.Value('Ok'),
        msg="put data successfully the grpc server!")
    else:
      return data_pb2.Response(
        code=data_pb2.StatusCode.Value('Failed'),
        msg="this node is full")

  def getHandler(self, request, context):
    params = {
      'from_utc': request.getRequest.queryParams.from_utc,
      'to_utc': request.getRequest.queryParams.to_utc,
      'target': 'mesowest' # default to mesowest
    }
    logging.warning('this is a get request with params %s' % str(params))
    if pre_read_check(params):
      for datFrag in try_read(params):
        logging.warning(datFrag)
        yield data_pb2.Response(
          code=data_pb2.StatusCode.Value('Ok'),
          msg="get data successfully from the grpc server!",
          datFragment=data_pb2.DatFragment(data=datFrag))
    else:
      yield data_pb2.Response(
        code=data_pb2.StatusCode.Value('Failed'),
        msg="Something wrong!")

  def ping(self, request, context):
    logging.warning('this is a ping request')
    return data_pb2.Response(
      code=data_pb2.StatusCode.Value('Ok'),
      msg="pinged successfully the grpc server!")
