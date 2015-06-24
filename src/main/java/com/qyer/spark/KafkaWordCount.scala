package com.qyer.spark

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.JedisPool


/**
 * kafka 测试wordcount
 */
class KafkaWordCount {
  //执行那个spark任务
  //./spark-submit --class "KafkaWordCount" --master "spark://boss:7077" --jars $(echo /root/wangzhen/all_lib/*.jar | tr ' ' ',') --total-executor-cores 2  /root/wangzhen/test1/test1.jar master:2181/cloud/kafka 1 test_api_response 8

}

object KafkaWordCount{

  implicit val formats = DefaultFormats
  case class Mailserver(uid: String,servertime: String, api_response: String,platform: String,api_name:String,context:String,debug:String,unique_id:String,app_name:String)

  val logger = Logger.getLogger(KafkaWordCount.getClass.getName)

  //获取json数据返回
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    // 创建StreamingContext，1秒一个批次


    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    logger.info("开始执行")
    val uids=lines.map(line => {
      val json = parse(line)
      json.extract[Mailserver].uid
    })

    val pairs = uids.map(uid => (uid, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords =>{
        partitionOfRecords.foreach(pair =>{
          val uid=pair._1
          val count=pair._2
          val jedis=RedisClient.pool.getResource
          jedis.hsetnx("app_open",uid,count.toString)
          RedisClient.pool.returnResource(jedis)
        })
      })
    })
    logger.info("写入redis完成")


    //wordCounts.print()

    ssc.start()            // 开始
    ssc.awaitTermination()  // 计算完毕退出

  }

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

//  def main(args: Array[String]) {
//    logger.info("测试")
//    val jedis=RedisClient.pool.getResource
//    //jedis.set("name","mm")
//    jedis.hsetnx("app_open","234567","mmm")
//    val k=jedis.hkeys("app_open").toArray()
//    for (i <- k){
//
//      println(i+"---"+jedis.hget("app_open",i.toString))
//    }
//
//    RedisClient.pool.returnResource(jedis)
//  }

  //获取worcount
//  def main(args: Array[String]) {
//
//    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
//
//    // 创建StreamingContext，1秒一个批次
//
//
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
//
//    val Array(zkQuorum, group, topics, numThreads) = args
//    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
//    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
//
//    // 对每一行数据执行Split操作
//    val words = lines.flatMap(_.split(" "))
//    // 统计word的数量
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//
//    //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//
//    // 输出结果
//    wordCounts.print()
//
//
//    ssc.start()            // 开始
//    ssc.awaitTermination()  // 计算完毕退出
//  }
}