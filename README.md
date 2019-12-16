# ChordAkka
A Chord implementation based on Java [Akka](https://akka.io/).

# Prerequisites 
- [Maven](https://maven.apache.org/)
- Java 11 or higher (openjdk)

## How to run

### With maven:
`mvn clean packge -DskipTests`

`cd target`

`java -Dconfig.resource=/centralNode.conf -jar chord-1.0-allinone.jar`

### With docker:

#### Start A Central Node

- `docker-compose up`

#### Start A Node:

- `./start_regular_node.sh $NODE_ID $IP`

#### Benchmark With Memcache:

- `docker-compose run benchmark --protocol=memcache_text --threads=1 --port 11211 --server=centralnode`

There are 2 types of nodes, a central and a regular node. The central node is used to join the network, this node should be started as the first node in the network so that others can join.

To run central node use parameter: `-Dconfig.resource=/centralNode.conf`

To run regular nodes use parameter: `-Dconfig.resource=/regularNode.conf`

To set your own node id specify `NODE_ID=x` in the environment variables.

Central node: `docker-compose up` 
Example for regular node: `./start_regular_node.sh 350 172.21.0.6`

## Testing the Memcached Interface
Either issue the following commands:
`echo -e 'set WeLike 0 60 19\r\nDistributed Systems\r\n' | nc localhost 11211`
`echo -e 'get WeLike\r\n' | nc localhost 11211`

Or use a TCP-connection such as telnet:
```
telnet
toggle crlf
open 127.0.0.1:11211
set WeLike 0 60 19
Distributed Systems

get WeLike

delete WeLike
```
## Benchmarking with Memcache Protocol

`memcslap --servers 127.0.0.1 --test=get`
`memtier_benchmark --protocol=memcache_text --port 11211 --server=127.0.0.1`

## Benchmarking in Docker

`docker-compose run benchmark --protocol=memcache_text -t=1 --port 11211 --server=centralnode`

## Open Telnet in Docker

`docker-compose run shell`

`telnet open centralnode 11211`
and all the fun commands

## Want to contribute?
### Branching
Branching will be done according the [Feature Branching Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow).

### Code Formatting
Standard IntelliJ java formatting profile

### Useful links
- https://doc.akka.io/docs/akka/current/typed/guide/index.html
