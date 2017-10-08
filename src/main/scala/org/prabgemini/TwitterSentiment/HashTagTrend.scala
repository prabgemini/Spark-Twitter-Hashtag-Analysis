package org.prabgemini.TwitterSentiment

import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import java.util.{Date, Properties}
import scala.util.Random

object HashtagTrend {
  def main(args: Array[String]) {
    
    //Parameters for Kafka Consumer connect
    val zkQuorum="52.171.131.173:2181"
    val topics="twitter-topic"
    val numThreads=1
    val group = "twitter-sample"
    
    //Spark Context initialization
    val sparkConf = new SparkConf().setAppName("twitterhashtag").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))
    
    //Create Kafka Dstream 
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val stream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      
    
    // Split the stream on space and extract hashtags 
    val hashTags = stream.flatMap(_.split(" ").filter(_.startsWith("#")))

    // Get the top hashtags over the previous 60 sec window
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))
 
    // Get the top hashtags over the previous 10 sec window
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

      
    // print tweets in the currect DStream 
    stream.print()
    
    
    //Aggregate the results and print hashtags for last 60 seconds
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
      
    })
    
    
    //Aggregate the results and print hashtags for last 10 seconds
    topCounts10.foreachRDD(rdd => {
        val topList = rdd.take(10)
        println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
        topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    
      })
  
   // Kafka Producer for writing results
    val events = 1000
    val topic = "spark-result"
    val brokers = "52.171.131.173:9092"
    val rnd = new Random()
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "ScalaSparkResult")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    
    
    
    topCounts60.foreachRDD(rdd => {
        val topList = rdd.take(10)
        //println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
        val today = new Date().getTime()
        topList.foreach { case (count, tag) => producer.send(new ProducerRecord[String, String](topic, today.toString() + "," + tag + "," + count)) }
      
      })
      
      
      ssc.start()
      ssc.awaitTermination()
   }
}