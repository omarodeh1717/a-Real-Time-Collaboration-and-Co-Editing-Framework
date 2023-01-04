package org.rcs

import com.mongodb.client._
import com.mongodb.client.model.Indexes
import com.mongodb.spark.MongoSpark

import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession

import java.util
import org.bson.Document

import scala.collection.mutable.ArrayBuffer

object Consumer {
  def main(args:Array[String]): Unit = {
    // Disable warnings
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    // Configure mongodb
    println("Configure mongodb")
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/kafka-records.record")
      .config("spark.mongodb.output.database", "kafka-records")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2")
      .getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


    // Configure kafka
    val kafkaServerURL = "localhost"
    val kafkaServerPort = "9092"

    println("Setting up parameters")
    val props = new Properties()
    props.put("bootstrap.servers", kafkaServerURL + ":" + kafkaServerPort)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "TrainingConsumer");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    println("Creating Consumer")
    val consumer = new KafkaConsumer[String, Array[Byte]](props)
    val listener = new RebalanceListener
    consumer.subscribe(util.Arrays.asList("add", "edit", "delete", "undo"), listener)

    println("Starting Consumer")
    while (true) {
      val records = consumer.poll(1000)
      val it = records.iterator()
      val recordsArray = new ArrayBuffer[Document]()
      while (it.hasNext) {
        val record = it.next()
        println("Received message:" + record)
        val doc = new Document()
        doc.put("topic", record.topic())
        doc.put("timestamp", record.timestamp())
        doc.put("offset", record.offset())
        doc.put("key", record.key())
        doc.put("value", record.value())
        recordsArray.append(doc)
      }
      val docRDD = spark.sparkContext.parallelize(recordsArray)
      MongoSpark.save(docRDD) // Uses the SparkConf for configuration
    }
  }
}
