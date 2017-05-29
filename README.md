# Benchmark-dbs

## A benchmark between [MongoDB](https://www.mongodb.com/), [ArangoDB](https://www.arangodb.com/) and [Aerospike](http://www.aerospike.com/) 

Using [Vertx](http://vertx.io/) and [Kotlin](https://kotlinlang.org/)

Main operations:
* Insert Data from CSV file
* FullScan from that data
* Search documents/bins using a specific cell data ("WHERE")
* Search documents/bins using several data from specific cell ("IN")
* Search documents/bins using range from specific cell ("BETWEEN")
* Aggregate documents/bins using group function ("GROUP BY") 
* Aggregate documents/bins using aggregation function ("SUM")

## Before Running
Start DBs. I used docker images:

`docker run -p 27018:27017 mongo`

`docker run -p 8529:8529 -e ARANGO_NO_AUTH=1 arangodb/arangodb:3.1.19`

`docker run -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike/aerospike-server:3.12.1`

## Compiling and Running
Use maven `mvn clean install` to compile and generate fat.jar file and then use `java -jar target/benchmark-dbs-1.0-SNAPSHOT-fat.jar -conf src/main/conf/conf.json` to apply configuration
