# cmpe275-spring18

RFC: Climate Facts Big Data Storage and Query Pipeline

## FOR THE DEMO:
* make sure you have docker installed, and then run this:
```
cd base_image/
docker build -t cmpe275_base_image .
cd database_node/
docker build -t cmpe275_db_base_image database_node/db_base_image/
```

* for database node - Ankit and Ryan make sure they can run this:
```
docker-compose build db
docker-compose up -d db

# to ssh into the container:

docker exec -it cmpe275_db bash

# to tail the logs in the container:

tail -f /srv/logs/*.log

```

### whenever the IP addr changes or is assigned to your node, do the following:

* for Ryan and Ankit, update /srv/src/constants.py with Haoji and Jason's new IP, go to localhost:9003, and then restart requestHandler
* for Jason, do nothing, just tell Ryan Ankit and Haoji what his new IP address is
* for Haoji, update /srv/src/constants.py, go to localhost:9004, and then restart grpcServer, tell Ryan and Ankit his new IP


## TODO: implement some sort of Raft
## TODO: same data query multiple times
## TODO: mongodb OpLog replication?

## Business Goal:

A weather data service for user to:

* upload data through an API
* query data through an API

## Engineering Goal:

* A distributed database that supports raw/text/video/image storage
* A python AND java API for user to upload/query the data.


## Design:

### Some Important Decisions
* hash(<timestamp_utc>+<station_name>) - unique key for each row to avoid duplicates
* socket connection circuit breaker pattern, retry periodically until the server is up
* all logs are pipelined to stdout, which is then collected by supervisord

### Architecture
https://www.lucidchart.com/documents/edit/1cbc95a8-b1b8-4bd7-b40c-52ca36e420b6/0


### MongoDB schema

```
main_db.weather.insert_one(
  {
    "station": // station name
    "timestamp_utc": // the weather data were gathered at
    "raw": // all columns except the station column
    "created_at_utc": // this row is inserted at
  }
  )

```
### Future TODOs:
* sharding on data
* cache on web server
* database oplog replication

### useful links

https://en.wikipedia.org/wiki/State_machine_replication
https://en.wikipedia.org/wiki/Paxos_(computer_science)
https://raft.github.io
https://en.wikipedia.org/wiki/Byzantine_fault_tolerance


https://stackoverflow.com/questions/4906977/access-environment-variables-from-python

https://docs.mongodb.com/manual/reference/limits/

https://stackoverflow.com/questions/24318084/flask-make-response-with-large-files
http://api.zeromq.org/3-2:zmq-proxy
https://docs.python.org/2/library/sqlite3.html

http://zguide.zeromq.org/page:all#toc14

http://zguide.zeromq.org/py:msgqueue

### Read, from web server to task scheduler proxy to actual db nodes
* Client - Proxy - Server Structure

### Write, pub-sub where web server publishes, all db nodes subscribe

### sqlite db schema for task scheduler to store all the nodes
```
CREATE TABLE nodes (
    [ip_addr] text PRIMARY KEY NOT NULL,
    [flags] integer,
    [created_at] text,
    [updated_at] text)
```
### sample usage:
```
>>> conn = sqlite3.connect('tmp.db')
>>> c = conn.cursor()
>>> c.execute('select * from etl_jobs;')
<sqlite3.Cursor object at 0x1030b4260>
>>> print c.fetchone()
(1, None, None, None, None, u'10,2,1,5', u'/tmp/23sdclw.gz')
>>>
```

### quick playground, this brings up a fresh ubuntu container immediately for you to play with:
```
docker run -it ubuntu
```


## Set up local dev env

* if your docker-compose hangs, try `127.0.0.1 localunixsocket localunixsocket.local # these are for docker `
* Fork and clone this: https://github.com/haojiliu/cmpe275-spring18

* On local do these:

```
cd base_image/
docker build -t cmpe275_base_image .
cd database_node/
docker build -t cmpe275_db_base_image database_node/db_base_image/
cd project1 && ./boostrap.sh
```

### To run javascript:
```
brew install maven
mvn clean package
java -cp target/grpcJava-1.0-SNAPSHOT-jar-with-dependencies.jar com.cmpe275.grpcComm.JavaClient
```

### To monitor processes on db node:
```
localhost:9003
```

### To monitor processes on task scheduler:
```
localhost:9002
```

### To monitor processes on grpc:
```
localhost:9004
```

### To view logs on a given node:
* ssh into the node
* `tail -f /srv/logs/*.log`


### To ssh into a container:
```
docker exec -it cmpe275_web bash
```

### Moreover, to scale 3 node db cluster for example:
```
docker-compose up --scale db=3
```

### play with db server:
```
python3 /srv/src/long_running_task.py
```

### debug web server:
```
tail -f /srv/logs/*.log -f /var/log/nginx/*.log
```

### useful docker cmds
```
docker system prune -a  # purge all to start over if your local is messed up
docker ps -a

docker-compose top
docker-compose build --no-cache # to rebuild everything from scratch
```

## Security

* ssh key
* auth or limited access for a given IP range
* password

## Performance

* rate limitation - not done yet
* load balance - not done yet
* stress test - not started yet

## Testing:

## Roadmap:

## Notes

## Ditched ideas:
Cassandra, no large files
Elassandra
RESTful requests, similar to grpc, but harder to do streaming
S3 for large file storage, extra cost
MongoDB, not distributed, no built in temporal spatial indexing
