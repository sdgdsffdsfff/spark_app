name := "spark_app"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies +="org.apache.spark" % "spark-streaming_2.10" % "1.3.1"

libraryDependencies +="org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.1"

libraryDependencies +="org.apache.spark" % "spark-core_2.10" % "1.3.1"

libraryDependencies +="org.json" % "json" % "20141113"

libraryDependencies +="log4j" % "log4j" % "1.2.17"

libraryDependencies +="org.apache.thrift" % "libthrift" % "0.9.2"

libraryDependencies +="redis.clients" % "jedis" % "2.7.2"





