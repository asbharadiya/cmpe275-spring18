# Author: Haoji Liu
import uuid, datetime
import os, re

from data_pb2 import Request, PutRequest, DatFragment, MetaData

"""This takes in a data file, normalize it to the standard data pattern we have."""

CONST_MEDIA_TYPE_TEXT_MESOWEST = 'mesowest'
CONST_MEDIA_TYPE_TEXT_MESONET = 'mesonet'

# Looks like 1KB is a good chunk size ~10 lines
CONST_CHUNK_SIZE = 3  # number of lines per payload

CONST_MESOWEST_HEADER = 'STN YYMMDD/HHMM MNET SLAT SLON SELV TMPF SKNT DRCT GUST PMSL ALTI DWPF RELH WTHR P24I'
CONST_MESONET_HEADER = '# id,name,mesonet,lat,lon,elevation,agl,cit,state,country,active'

CONST_HEADERS = (CONST_MESOWEST_HEADER, CONST_MESONET_HEADER)

# only station, time, lat, lon, elv exist in mesonet data
CONST_MESONET_STR = '%s,%s,NULL,%s,%s,%s,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL'
CONST_MESONET_TO_STD_MATCHING_COLS = (0,1,4,5,6) # these columns have matching columns in mesowest


CONST_TIMESTAMP_FMT = '%Y-%m-%d %H:%M:%S'

CONST_MESONET_DELIMITER = ','

CONST_DELIMITER = ','
CONST_NEWLINE_CHAR = '\n'

# station, timestamp_utc, mnet??, latitude, longitude, temperature, ...
CONST_STD_COL_LIST = CONST_MESOWEST_HEADER.split()
CONST_MESONET_COL_LIST = ['id','name','mesonet','lat','lon','elevation','agl','cit','state','country','active']


def format_timestamp(timestamp):
  """
  convert from 20180316_2145 to 2018-03-16 21:45:00
  """
  try:
    tuples = re.split('_|/', timestamp)
    assert len(tuples) == 2
    year = tuples[0][:4]
    month = tuples[0][4:6]
    day = tuples[0][6:8]
    hour = tuples[1][:2]
    minute = tuples[1][2:4]

    res = '%s-%s-%s %s:%s:00' % (year, month, day, hour, minute)
    # make sure it's a valid timestamp
    datetime.datetime.strptime(res, CONST_TIMESTAMP_FMT)
    return res

  except:
    return None

def format_timestamp_mesowest(timestamp):
  """
  convert from 20180316/2145 to 2018-03-16 21:45:00
  """
  tuples = timestamp.split('/')
  assert len(tuples) == 2
  year = tuples[0][:4]
  month = tuples[0][4:6]
  day = tuples[0][6:8]
  hour = tuples[1][:2]
  minute = tuples[1][2:4]

  return '%s-%s-%s %s:%s:00' % (year, month, day, hour, minute)

def _normalize_mesonet(line, timestamp_utc):
  """Map mesonet to our standard format"""
  # add timestamp
  cols = line.split(CONST_MESONET_DELIMITER)
  assert len(cols) == len(CONST_MESONET_COL_LIST)

  new_cols = cols[:1] + [timestamp_utc] + cols[1:]
  matching_cols = []
  # constructing a list of matching values
  for idx in CONST_MESONET_TO_STD_MATCHING_COLS:
    matching_cols.append(new_cols[idx])
  # fill in values and return
  return CONST_MESONET_STR % tuple(matching_cols)

def _normalize_mesowest(line):
  """Map mesowest to our standard format"""
  # replace timestamp with standardized one
  cols = line.split()
  assert len(cols) == len(CONST_STD_COL_LIST)

  timestamp_utc = format_timestamp_mesowest(cols[1])
  return CONST_DELIMITER.join(cols[:1] + [timestamp_utc] + cols[2:])

def normalize(line, data_source, timestamp_utc):
  if data_source == CONST_MEDIA_TYPE_TEXT_MESONET:
    assert timestamp_utc is not None
    return _normalize_mesonet(line, timestamp_utc)
  elif data_source == CONST_MEDIA_TYPE_TEXT_MESOWEST:
    return _normalize_mesowest(line)
  else:
    print('unsupported data format')

def parse_file(fpath):
  """read file and chunkify it to be small batch for grpc transport

  Returns: a string, concat of data rows, separated by newline char
  """
  buffer = []
  is_starts_reading = False
  is_mesonet = False

  filename, file_extension = os.path.splitext(fpath.split('/')[-1])
  timestamp_utc = format_timestamp(filename)
  is_mesonet = timestamp_utc is not None
  with open(fpath) as f:
    for line in f:
      if not is_starts_reading:
        if is_mesonet:
          possible_header = line.strip()
        else:
          possible_header = ' '.join(line.strip().split())
        # For both mesonet and mesowest
        if possible_header in CONST_HEADERS:
          is_starts_reading = True
          # skip this line
          continue

      if not is_starts_reading:
        continue

      data_source = CONST_MEDIA_TYPE_TEXT_MESONET if is_mesonet else CONST_MEDIA_TYPE_TEXT_MESOWEST
      try:
        normalized_line = normalize(line.strip(), data_source, timestamp_utc)
      except Exception as e:
        print('skipping line: %s' % line)
        continue
      buffer.append(normalized_line)

      if len(buffer) == CONST_CHUNK_SIZE:
        res = CONST_NEWLINE_CHAR.join(buffer)
        buffer = []
        yield res
    # last batch
    if buffer:
      yield CONST_NEWLINE_CHAR.join(buffer)

def put_req_iterator(fpath, sender, receiver):
  my_uuid = str(uuid.uuid1())
  for raw in parse_file(fpath):
    yield Request(
      fromSender=sender,
      toReceiver=receiver,
      putRequest=PutRequest(
          metaData=MetaData(uuid=my_uuid),
          datFragment=DatFragment(data=raw.encode()))
      )

if __name__ == '__main__':
  #for chunk in parse_file('../mesowesteasy.out'):
  for chunk in parse_file('../20170101_1030.csv'):
    print('--------')
    print(chunk)
