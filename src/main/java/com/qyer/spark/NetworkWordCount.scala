package com.qyer.spark


import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by wangzhen on 15/6/10.
 */
class NetworkWordCount {


}

object NetworkWordCount {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")

    // 创建StreamingContext，1秒一个批次


    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 获得一个DStream负责连接 监听端口:地址
    //val lines = ssc.textFileStream("workspace/bbs/")
    //val lines=ssc.socketTextStream("172.1.40.10",9999,StorageLevel.MEMORY_AND_DISK_SER)
    val lines=ssc.socketTextStream("10.6.17.228",9999,StorageLevel.MEMORY_AND_DISK_SER)


    // 对每一行数据执行Split操作
    val words = lines.flatMap(_.split(" "))
    // 统计word的数量
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    // 输出结果
    wordCounts.print()

    ssc.start()            // 开始
    ssc.awaitTermination()  // 计算完毕退出

  }

}