# hadoop-docker-demo
A sample application with hadoop and docker.

This sample application shows how to use Docker to create a Hadoop cluster and a Big Data application in Java. It highlights several concepts like service scale, dynamic port allocation, container links, integration tests, debugging, etc.

## Running Hadoop and our application:


Compile the application and generate the docker images

```
cd sample
mvn clean install -Papp-docker-image
``` 


Start all the services 

```
docker-compose --file docker/docker-compose.yml up -d
```

Open `http://localhost:8088/cluster` to see your if your cluster is running. You should see 1 active node when everything is up.

If you want, you can scale your cluster, adding more Hadoop nodes to it:
```
docker-compose --file docker/docker-compose.yml scale nodemanager=2
```
Go to `http://localhost:8088/cluster` and refresh until you see 2 active nodes.


Create a folder on hdfs to test

```
docker-compose --file docker/docker-compose.yml exec yarn hdfs dfs -mkdir /files/
```

Put the file we are going to process on hdfs

```
docker-compose --file docker/docker-compose.yml run docker-hadoop-example hdfs dfs -put /maven/test-data/text_for_word_count.txt /files/
```

Run our application
```
docker-compose --file docker/docker-compose.yml run docker-hadoop-example hadoop jar /maven/jar/docker-hadoop-example-1.0-SNAPSHOT-mr.jar hdfs://namenode:9000 /files mongo yarn:8050
```

Stop all the services
```
docker-compose --file docker/docker-compose.yml down
``` 
