package org.rcs.client

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.rcs.framework.{RCSClient, RCSConfig, RCSSession}

import collection.JavaConverters._
import java.util.Properties
import scala.collection.JavaConverters.asJavaIterableConverter

/**
 * @author ${user.name}
 */
object Producer {

  var operationStart: Long = System.currentTimeMillis()
  var isConsumerStartedListening: Boolean = false

  def main(args : Array[String]) {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    val fileName = "C:\\IdeaProjects\\test\\doc1.txt"



    startListeningForUpdates(Array(fileName));
    try {
      val rcs = new RCSSession(new RCSConfig(fileName))

      while (!isConsumerStartedListening) {
        Thread.sleep(100)
      }
     /* while (true) {
        print("Enter user: ")
        val user = scala.io.StdIn.readLine()
        print("Enter operation: ")
        val operation = scala.io.StdIn.readLine()
        print("Enter start position: ")
        val startPosition = scala.io.StdIn.readLine().toInt
        if (operation.equals("add")) {
          print("Enter value: ")
          val value = scala.io.StdIn.readLine()
          rcs.add(user, Left(value), startPosition)
          operationStart = System.currentTimeMillis()
        } else if (operation.equals("edit")) {
          print("Enter value: ")
          val value = scala.io.StdIn.readLine()
          rcs.edit(user, Left(value), startPosition)
          operationStart = System.currentTimeMillis()
        } else if (operation.equals("delete")) {
          val endPosition = scala.io.StdIn.readLine().toInt
          print("Enter end position: ")
          rcs.delete(user, startPosition, endPosition)
          operationStart = System.currentTimeMillis()
        }
      }*/
      operationStart = System.currentTimeMillis()
      rcs.add("User1", Left("This is a greet way to enhance the propuction."), 0)
      rcs.edit("User1", Left("a"), 13)
      rcs.edit("User 2", Left("production."), 35)
      rcs.add("User 2", Left(" But, it comes with a great danger."), 46)
      rcs.delete("User 1", 68, 74)



    } finally {
      RCSClient.close()
    }
  }

  def startListeningForUpdates(filesNames: Array[String]): Unit = {
    val thread = new Thread {
      override def run(): Unit = {
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

        println("Creating Consumer to listen for updates")
        val consumer = new KafkaConsumer[String, Array[Byte]](props)
        val listener = new RebalanceListener

        val escaped_fileNames = filesNames.map(file => file.replace(':', '_').replace('\\', '-')).toList.asJavaCollection
        consumer.subscribe(escaped_fileNames, listener)

        println("Starting Consumer to listen for updates")
        while (true) {
          val records = consumer.poll(100)

          // Set it to true after the first poll
          isConsumerStartedListening = true
          val it = records.iterator()
          if(it.hasNext) {
            println("Operation time: " + (System.currentTimeMillis() - operationStart) + "ms")
          }
          while (it.hasNext) {
            val record = it.next()
            println("Received update on file:" + record.key() + " Updated file: " + record.value().map(_.toChar).mkString)
          }
        }
      }
    }
    thread.start()
  }
}

/*
* while(true){
  print("Enter topic: ")
  val topic = scala.io.StdIn.readLine()
  print("Enter key: ")
  val key = scala.io.StdIn.readLine()
  print("Enter value: ")
  val value = scala.io.StdIn.readLine()
  RCSClient.sendRecord(topic, key, value.getBytes)
  println("Record sent successfully\n")
}
* */


