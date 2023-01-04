package org.rcs.framework

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties
import java.util.concurrent.Future

object RCSClient {
  var producer: KafkaProducer[String, Array[Byte]] = _

  def start(serverPort: Int = 9092) {
    if (producer == null) {
      val props: Properties = new Properties()
      props.put("bootstrap.servers", "localhost:" + serverPort)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      props.put("acks", "all")
      producer = new KafkaProducer[String, Array[Byte]](props)
    }
  }

  def sendRecord(topic: String, key: String, value: Array[Byte]): Future[RecordMetadata] = {
    val record = new ProducerRecord(topic, key, value)
    producer.send(record)
  }

  def close (): Unit = {
    if (producer != null) {
      producer.close()
    }
  }
}
