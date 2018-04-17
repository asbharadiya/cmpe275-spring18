echo "registering cluster..."
myip="$(ifconfig | grep -A 1 'en0' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 2)"
echo "writing to bluemix: $myip"
url="https://cmpe275-spring-18.mybluemix.net/put/$myip"
curl $url
echo "build cluster..."
docker-compose build --no-cache
echo "bring up cluster..."
docker-compose up -d
