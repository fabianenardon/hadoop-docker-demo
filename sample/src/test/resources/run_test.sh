
# Generates the images
mvn clean install -Papp-docker-image

# Starts all the services
docker-compose --file src/test/resources/docker-compose.yml up -d

# Waits for services do start
sleep 30

# Creates the folder we need on hdfs to test
docker-compose --file src/test/resources/docker-compose.yml exec yarn hdfs dfs -mkdir /files/

# Put the file we are going to process on hdfs
docker-compose --file src/test/resources/docker-compose.yml run docker-hadoop-example hdfs dfs -put /maven/test-data/text_for_word_count.txt /files/

# Run our application
docker-compose --file src/test/resources/docker-compose.yml run docker-hadoop-example hadoop jar /maven/jar/docker-hadoop-example-1.0-SNAPSHOT-mr.jar hdfs://namenode:9000 /files mongo yarn:8050

# Run our integration tests
docker-compose --file src/test/resources/docker-compose.yml run docker-hadoop-example-tests mvn -f /maven/code/pom.xml -Dmaven.repo.local=/m2/repository -Pintegration-test verify 
# If you want to remote debug tests, run instead
# docker run -v ~/.m2:/m2 -p 5005:5005 --link mongo:mongo --net resources_default docker-hadoop-example-tests mvn -f /maven/code/pom.xml -Dmaven.repo.local=/m2/repository -Pintegration-test verify -Dmaven.failsafe.debug


# Stop all the services
docker-compose --file src/test/resources/docker-compose.yml down
