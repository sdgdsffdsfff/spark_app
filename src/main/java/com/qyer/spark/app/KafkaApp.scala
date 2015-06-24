package com.qyer.spark.app

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisPool

/**
 * Created by wangzhen on 15/6/17.
 */
class KafkaApp {
  //执行那个spark任务
  //./spark-submit --class "com.qyer.spark.app.KafkaApp" --master "spark://boss:7077" --jars $(echo /root/wangzhen/all_lib/*.jar | tr ' ' ',') --total-executor-cores 6  /root/wangzhen/test1/test1.jar master:2181/cloud/kafka 1 app_open 4
}

object KafkaApp{
  def logger = Logger.getLogger(KafkaApp.getClass.getName)

  object RedisClient extends Serializable {
    //val redisHost = "172.1.1.186"
    val redisHost = "master"
    val redisPort = 6379
    val redisTimeout = 30000
    lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

    lazy val hook = new Thread {
      override def run = {
        println("Execute hook thread: " + this)
        pool.destroy()
      }
    }
    sys.addShutdownHook(hook.run)
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaApp")
    // 创建StreamingContext，1秒一个批次


    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val Array(zkQuorum, group, topics, numThreads) = args
    //val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val topicMap = Map(topics -> 1)
    val kafkaDStreams = {
      val streams = (1 to numThreads.toInt).map { _ =>
        KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      }
      val unionDStream = ssc.union(streams)
      val sparkProcessingParallelism = 1
      unionDStream.repartition(sparkProcessingParallelism)
    }

    //val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    logger.info("开始执行")
    val results=kafkaDStreams.map(_.split("\\#\\|\\~"))
    val result=results.map(line =>{
      if (line.size >3)
        (line(1),line(2),line(3))
      else
        (0,0,0)
    })


    result.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords =>{
        partitionOfRecords.foreach(pair =>{
//          val deviceid=pair._1
//          val lat=pair._2
//          val lon=pair._3
//          val jedis=RedisClient.pool.getResource
//          if (!lat.equals("") && !lon.equals("")){
//            logger.info("记录存储……，deviceid:"+deviceid)
//            jedis.hsetnx("app_open",deviceid.toString,lat.toString+"-"+lon.toString)
//            RedisClient.pool.returnResource(jedis)
//          }

          object InternalRedisClient extends Serializable {

            @transient private var pool: JedisPool = null

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
              makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
            }

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
                         testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
              if(pool == null) {
                val poolConfig = new GenericObjectPoolConfig()
                poolConfig.setMaxTotal(maxTotal)
                poolConfig.setMaxIdle(maxIdle)
                poolConfig.setMinIdle(minIdle)
                poolConfig.setMinIdle(minIdle)
                poolConfig.setTestOnBorrow(testOnBorrow)
                poolConfig.setTestOnReturn(testOnReturn)
                poolConfig.setMaxWaitMillis(maxWaitMillis)
                pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

                val hook = new Thread{
                  override def run = pool.destroy()
                }
                sys.addShutdownHook(hook.run)
              }
            }

            def getPool: JedisPool = {
              assert(pool != null)
              pool
            }
          }

          // Redis configurations
          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "master"
          val redisPort = 6379
          val redisTimeout = 30000
          val dbIndex = 1
          InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

          val deviceid=pair._1
          val lat=pair._2
          val lon=pair._3
          val jedis=InternalRedisClient.getPool.getResource
          if (!lat.equals("") && !lon.equals("")){
            logger.info("记录存储……，deviceid:"+deviceid)
            jedis.hsetnx("app_open",deviceid.toString,lat.toString+"-"+lon.toString)
            InternalRedisClient.getPool.returnResource(jedis)
          }
        })
      })
    })
    ssc.start()            // 开始
    ssc.awaitTermination()  // 计算完毕退出
  }

//  def main(args: Array[String]) {
//    val s="0#|~1B828163-2251-4D8E-ADB0-ADE4EC70FE82#|~23.32224923200873#|~107.5861455080933#|~1434330000#|~6.3#|~App%20Store#|~iPhone7,1#|~ios%208.3#|~#|~#|~#|~1434330000#|~qyer_ios#|~cd254439\n208ab658ddf9#|~#|~#|~#|~#|~1434328898#|~#|~#|~222.218.211.35#|~/qyer/recommands/trip#|~a:11:{s:6:\"action\";s:4:\"trip\";s:15:\"app_installtime\";s:10:\"1434328898\";s:5:\"count\";s:2:\"10\";s:\n4:\"page\";s:1:\"2\";s:4:\"type\";s:5:\"index\";s:1:\"v\";s:1:\"1\";s:6:\"__utma\";s:54:\"253397513.530962400.1434328948.1434328948.1434328948.1\";s:6:\"__utmb\";s:25:\"253397513.1.10.1434328948\";s:6:\n\"__utmz\";s:70:\"253397513.1434328948.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)\";s:5:\"_guid\";s:36:\"04af6001-dcae-8bbf-c047-aba0208ad0a0\";s:8:\"_session\";s:13:\"1434328918255\";}"
//    println(s.split("\\#\\|\\~")(3))
//  }
}