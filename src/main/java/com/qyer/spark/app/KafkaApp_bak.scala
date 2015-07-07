package com.qyer.spark.app

import com.qyer.rpc.dataservice.{RedisUtil, IpforCity, CityInfo, CommonServiceServer}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.thrift.protocol.{TProtocol, TBinaryProtocol}
import org.apache.thrift.transport.{TTransport, TSocket}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s._
import redis.clients.jedis.{Jedis, JedisPool}
import redis.clients.jedis.exceptions.{JedisException, JedisConnectionException}


/**
 * Created by wangzhen on 15/6/17.
 */
class KafkaApp_bak {
  //执行那个spark任务
  // /root/cloud/spark/bin/spark-submit --class "com.qyer.spark.app.KafkaApp" --master "spark://boss:7077" --jars $(echo /root/wangzhen/all_lib/*.jar | tr ' ' ',') --total-executor-cores 4  /root/wangzhen/spark_app/spark_app.jar master:2181/cloud/kafka 1 app_api 2
}

object KafkaApp_bak{
  def logger = Logger.getLogger(KafkaApp.getClass.getName)

  implicit val formats = DefaultFormats
  case class Mailserver(track_deviceid: String,lat: String, lon: String,ip: String)

  /**
   * 创建redis连接实例，但是多个分区输出时会出现任务堵死情况，暂不使用
   * @param args
   */
  //  object RedisClient extends Serializable {
  //    //val redisHost = "172.1.1.186"
  //    val redisHost = "master"
  //    val redisPort = 6379
  //    val redisTimeout = 30000
  //    lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)
  //
  //    lazy val hook = new Thread {
  //      override def run = {
  //        println("Execute hook thread: " + this)
  //        pool.destroy()
  //      }
  //    }
  //    sys.addShutdownHook(hook.run)
  //  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaApp")
    // 创建StreamingContext，1秒一个批次


    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //获取客户端传递的参数
    val Array(zkQuorum, group, topics, numThreads) = args

    val topicMap = Map(topics -> 1)
    //获取多个receiver union后的数据
    val kafkaDStreams = {
      //获取kafka输入的dstream，并按照分区启动numThreads个receivers
      val streams = (1 to numThreads.toInt).map { _ =>
        KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      }
      val unionDStream = ssc.union(streams)//进行union操作，即将多个receivers的记过union
      val sparkProcessingParallelism = 1
      unionDStream.repartition(sparkProcessingParallelism)
    }


    logger.info("开始执行")
    //    //按照分隔符进行分割不同的字段
    //    val results=kafkaDStreams.map(_.split("\\#\\|\\~"))
    //    //获取deviceid，经度、纬度
    //    val result=results.map(line =>{
    //      if (line.size >22)
    //        (line(1),line(2),line(3),line(22))
    //      else
    //        (0,0,0,0)
    //    })
    val result=kafkaDStreams.map(line =>{
      try {
        val json = parse(line)
        (json.extract[Mailserver].track_deviceid,json.extract[Mailserver].lat, json.extract[Mailserver].lon, json.extract[Mailserver].ip)
      }catch{
        case e:MappingException =>
          (0,0,0,0)
      }
    })


    //将按照分区进行数据的输出，输出到redis实例
    result.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords =>{
        partitionOfRecords.foreach(pair =>{

                    //创建redis实例
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
                    val maxTotal = 1024
                    val maxIdle = 50
                    val minIdle = 1
                    val redisHost = "master"
                    val redisPort = 6379
                    val redisTimeout = 30000
                    val dbIndex = 1
                    InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

          val deviceid=pair._1
          val lat=pair._2
          val lon=pair._3
          val ip=pair._4
          val cityName = {
            if (ip.toString.equals("0")){
              "0"
            }else{
              IpforCity.ip2CityId(ip.toString).getCity()
            }


          }
                    var jedis=new Jedis
                    try{
                      jedis=InternalRedisClient.getPool.getResource//获取redis的连接池
                      if (!lat.equals("") && !lon.equals("")){
                        logger.info("记录存储……，deviceid:"+deviceid)
                        jedis.hset("app_open",deviceid.toString,lat.toString+"|"+lon.toString+"|"+cityName.toString)

                      }
                    }catch{
                      case e:JedisConnectionException =>
                        InternalRedisClient.getPool.returnResourceObject(jedis)
                      case e:JedisException =>
                        InternalRedisClient.getPool.returnResourceObject(jedis)

                    }finally {
                      InternalRedisClient.getPool.returnResourceObject(jedis)

                    }



        })
      })
    })
    ssc.start()            // 开始
    ssc.awaitTermination()  // 计算完毕退出
  }


}