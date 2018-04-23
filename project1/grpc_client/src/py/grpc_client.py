# This is the grpc client, that will read a data file, chunk it into several messages, and send it to grpc server
# Haoji Liu
import grpc
import data_pb2_grpc
from data_pb2 import Request, Response, PingRequest, PutRequest, GetRequest, DatFragment, MetaData, QueryParams

import file_parser

CONST_NEWLINE_CHAR = '\n'
CONST_MESOWEST_HEADER = 'STN YYMMDD/HHMM MNET SLAT SLON SELV TMPF SKNT DRCT GUST PMSL ALTI DWPF RELH WTHR P24I'

class Client():
  def __init__(self, host, port, sender):
    assert sender is not None, 'sender has to be specified'
    self.channel = grpc.insecure_channel('%s:%d' % (host, port))
    self.stub = data_pb2_grpc.CommunicationServiceStub(self.channel)
    self.sender = sender
    self.receiver = host

  def ping(self, msg):
    """
    Returns: bool
    """
    req = Request(
      fromSender=self.sender,
      toReceiver=self.receiver,
      ping=PingRequest(msg=msg))
    resp = self.stub.ping(req)
    print(resp.msg)
    return True

  def put(self, fpath):
    """
    Returns: bool
    """
    print('putting to %s...' % self.receiver)
    req_iterator = file_parser.put_req_iterator(fpath, self.sender, self.receiver)
    resp = self.stub.putHandler(req_iterator)
    print(resp.msg)
    if resp.code == 2:
      print('write failed at this node!')
      return False
    print('returning true!')
    return True

  def get(self, fp, from_utc, to_utc, params_json):
    """
    Returns: bool
    """
    req = Request(
      fromSender=self.sender,
      toReceiver=self.receiver,
      getRequest=GetRequest(
          metaData=MetaData(),
          queryParams=QueryParams(from_utc=from_utc,to_utc=to_utc, params_json=params_json))
      )
    fp.write(CONST_MESOWEST_HEADER + CONST_NEWLINE_CHAR)

    for resp in self.stub.getHandler(req):
      if resp.code == 2:
        print('read failed at this node!')
        return False
      else:
        fp.write(resp.datFragment.data.decode() + CONST_NEWLINE_CHAR)

    return True
