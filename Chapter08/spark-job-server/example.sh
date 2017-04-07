# Create new context
curl -XPOST 'localhost:8090/contexts/gzet?num-cpu-cores=4&memory-per-node=4g&spark.executor.instances=2&spark.driver.memory=2g&passthrough.spark.cassandra.connection.host=127.0.0.1&passthrough.spark.cassandra.connection.port=9042'
curl -XGET 'localhost:8090/contexts'

# Upload Jar file
curl --data-binary @/Users/antoine/Workspace/gzet/recommender/recommender-core/target/recommender-core-1.0-SNAPSHOT.jar 'localhost:8090/jars/gzet'
curl -XGET 'localhost:8090/jars'

# Test Jar
curl -XPOST 'localhost:8090/jobs/test?appName=gzet&classPath=io.gzet.recommender.TestSparkServer&context=gzet&sync=true'
