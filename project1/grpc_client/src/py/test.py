# Haoji Liu
import argparse, datetime, json
import requests, argparse, socket, fcntl, struct
import api

"""This is the test script to test the API functionalities"""


# TODO: move to elsewhere
def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(s)
    my_ip = socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15].encode())
    )[20:24])
    return my_ip

def get_ip_address_mac():
  f = lambda x: x.startswith('169')
  ips = [ip for ip in socket.gethostbyname_ex(socket.gethostname()) if any(f(ip))]
  print(ips)
  return ips[0]
try:
  sender = get_ip_address_mac()
  print('my ip is %s' % sender)
except:
  sender = 'some host'


def main():
  """
  Sample Usage:
  get: -H 0.0.0.0 -P 8080 -g -t '2016-07-08 10:00:00' '2016-07-08 10:00:00'
  put: -H 0.0.0.0 -P 8080 -u -f './201803180010.mdf'
  ping: -H 0.0.0.0 -P 8080 -p -m 'hello world!'
  """
  parser = argparse.ArgumentParser(description='Weather Data Lake Python API v1.0')
  parser.add_argument('-H', '--host', type=str, default='0.0.0.0', help='The host of the grpc server')
  parser.add_argument('-P', '--port', type=int, default=8080, help='The port listened by grpc server')
  parser.add_argument('-f', '--file', type=str, default='../mesowesteasy.out', help='The file path to upload')
  parser.add_argument('-g', '--get', action='store_true', default=False, help='-g -t <from_utc> <to_utc>')
  parser.add_argument('-u', '--upload', action='store_true', default=False, help='Upload data to the server')
  parser.add_argument('-p', '--ping', action='store_true', default=False, help='Ping the server')
  parser.add_argument('-t', '--range', type=str, nargs=2, help='-t <from_utc> <to_utc>')
  parser.add_argument('-s', '--stations', nargs='*', help='-s <station1> <station2> <...>')
  parser.add_argument('-m', '--message', type=str, default='Hello World!', help='-m "Hello World!"')
  parser.add_argument('-o', '--output', type=str, default='./result.out', help='-m "Specify the output file locaton for queries"')
  parser.add_argument('-b', '--broadcast', action='store_true', default=False, help='-m "Put to all nodes if True, otherwise just put to the given host"')

  args = parser.parse_args()
  try:
    host = args.host
    port = args.port
    assert host
    assert port

    if args.get:
      assert args.range
      from_utc, to_utc = args.range[0], args.range[1]
      assert from_utc
      assert to_utc
      # params = json.dumps([{
      #     'lhs': 'TMPF',
      #     'op': 'gt',
      #     'rhs': '0'
      #   },
      #   {
      #     'lhs': 'STN',
      #     'op': 'eq',
      #     'rhs': ['HCOT1', 'BBN']
      #   }])
      params = ''
      return api.api_get(args.output, from_utc, to_utc, host, port, sender, params)

    elif args.upload:
      fp = args.file
      assert fp
      return api.api_put(fp, args.broadcast, host, port, sender)

    elif args.ping:
      return api.api_ping(args.message, host, port, sender)

  except Exception as e:
    print(e)
    parser.print_help()

if __name__ == '__main__':
  main()
