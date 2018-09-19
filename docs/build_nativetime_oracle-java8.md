### Run container

`$ docker pull store/oracle/serverjre:8`

`$ docker run -i -t store/oracle/serverjre:8 /bin/bash`

### Within the docker container:

`$ yum group install "Development Tools"`

`$ cd /usr/local/src`

`$ cat > nativetime.cpp <<EOF`

`$ cat > clock.h <<EOF`

`$ g++ -c -fPIC -O2 -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux nativetime.cpp -o nativetime.o`

`$ g++ -shared -fPIC -o libnativetime.so nativetime.o -lc`

ctrl-d to quit docker image

get the docker id using:
`$ docker ps -a`

`$ docker cp <docker_id>:/usr/local/src/libnativetime.so /tmp/`

`$ docker rm <docker_id>`

`$ docker run -i -t store/oracle/serverjre:8 /bin/bash`

get the docker id using:
`$ docker ps -a`

`$ docker cp /tmp/libnativetime.so <docker_id>:/usr/lib64/libnativetime.so`

### Within the docker container:

`$ cd /usr/lib64`

`$ chown root libnativetime.so`

`$ chgrp root libnativetime.so`

ctrl-d to quit docker image

`$ docker ps -a` (check the *CONTAINER ID*)

`$ docker commit <container_id> blokaly/java8`

`$ docker push blokaly/java8` (need docker hub login first: `$ docker login --username=xxx`)

 


