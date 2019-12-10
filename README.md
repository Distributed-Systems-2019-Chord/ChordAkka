# ChordAkka
A Chord implementation based on Akka

For now it's based on the following example: https://developer.lightbend.com/guides/akka-quickstart-java/index.html

# Prerequisites 
- [Maven](https://maven.apache.org/)
- Java 11 or higher (openjdk)

## How to run
`mvn clean packge -DskipTests`
`cd target`
`java -Dconfig.resource=/centralNode.conf -jar chord-1.0-allinone.jar`

Or use docker:
`docker-compose up`

To run central node use parameter: `-Dconfig.resource=/centralNode.conf`

To run regular nodes use parameter: `-Dconfig.resource=/regularNode.conf`

To set your own node id specify node.id=x in the environment variables.

Test run: first run central node then run the regular node

## Branching
Branching will be done according the [Feature Branching Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow).

## Useful links
- https://doc.akka.io/docs/akka/current/typed/guide/index.html

## Code Formatting
Standard IntelliJ java formatting profile

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
