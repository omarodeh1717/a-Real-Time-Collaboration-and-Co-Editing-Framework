package org.rcs


import com.mongodb.client._
import com.mongodb.client.model.{Filters, Updates}
import com.mongodb.client.model.Filters.{and, lt}
import com.mongodb.client.model.Sorts.{descending, orderBy}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.bson.Document

import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

object Processor {
  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put("acks", "all")
  val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer[String, Array[Byte]](props)

  def saveVersion(fileNames: List[String], versionsCollection: MongoCollection[Document]): Unit = {
    fileNames.foreach(fileName => {
      val bis = new BufferedInputStream(new FileInputStream(fileName))
      val bArray = Stream.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toArray
      val doc = new Document()
      doc.put("fileName", fileName)
      doc.put("content", bArray)
      doc.put("timestamp", System.currentTimeMillis())
      versionsCollection.insertOne(doc)
    })
  }


  def processBatch(records: ArrayBuffer[Record], collection: MongoCollection[Document]): Unit = {
    records.foreach(record => {
      if (record.topic == "add"){
        processAdd(record)
      } else if (record.topic == "edit") {
        processEdit(record, collection)
      } else if (record.topic == "delete") {
        processDelete(record, collection)
      } else if (record.topic == "undo") {
        processUndo(record, collection)
      }
    })
  }

  def processAdd(record: Record): Unit = {
    val fileName = record.command.session.fileName
    val value = record.data
    val startPosition = record.command.startPosition
    val bis = new BufferedInputStream(new FileInputStream(fileName))
    val bArray = Stream.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toBuffer
    bArray.insertAll(startPosition, value.toBuffer)
    val bos = new BufferedOutputStream(new FileOutputStream(fileName))
    Stream.continually(bos.write(bArray.toArray))
    bos.close() // You may end up with 0 bytes file if not calling close.

    // Send update to file users
    val topic = fileName.replace(':', '_').replace('\\', '-')
    producer.send(new ProducerRecord(topic, fileName, bArray.toArray))
  }

  def processEdit(record: Record, collection: MongoCollection[Document]): Unit = {
    val fileName = record.command.session.fileName
    val value = record.data
    val startPosition = record.command.startPosition
    val bis = new BufferedInputStream(new FileInputStream(fileName))
    val fileAsBytes = Stream.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toArray

    var i = 0
    val oldData = new Array[Byte](value.length)
    for( a <- startPosition to value.length + (startPosition -1)){
      oldData(i) = fileAsBytes(a)
      fileAsBytes(a) = value(i)
      i = i + 1
    }

    val bos = new BufferedOutputStream(new FileOutputStream(fileName))
    Stream.continually(bos.write(fileAsBytes))
    bos.close() // You may end up with 0 bytes file if not calling close.

    // Send update to file users
    val topic = fileName.replace(':', '_').replace('\\', '-')
    producer.send(new ProducerRecord(topic, fileName, fileAsBytes))

    collection.findOneAndUpdate(Filters.eq("_id", record.id), Updates.set("oldValue", oldData))
  }

  def processDelete(record: Record, collection: MongoCollection[Document]): Unit = {
    val fileName = record.command.session.fileName
    val startPosition = record.command.startPosition
    val endPosition = record.command.endPosition
    val bis = new BufferedInputStream(new FileInputStream(fileName))
    val bArray = Stream.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toBuffer
    val oldData = bArray.toArray.slice(startPosition,  endPosition)
    bArray.remove(startPosition, endPosition - startPosition)
    val bos = new BufferedOutputStream(new FileOutputStream(fileName))
    Stream.continually(bos.write(bArray.toArray))
    bos.close() // You may end up with 0 bytes file if not calling close.

    // Send update to file users
    val topic = fileName.replace(':', '_').replace('\\', '-')
    producer.send(new ProducerRecord(topic, fileName, bArray.toArray))

    collection.findOneAndUpdate(Filters.eq("_id", record.id), Updates.set("oldValue", oldData))
  }

  def processUndo(record: Record, collection: MongoCollection[Document]): Unit = {
    val lastOptDoc = collection.find(and(Filters.text(record.command.userId), lt("timestamp", record.timestamp))).sort(orderBy(descending("timestamp"))).first()
    val lastOperation = Utils.toRecord(lastOptDoc)
    if (lastOperation.topic == "add") {
      processDelete(lastOperation, collection)
    } else if (lastOperation.topic == "edit") {
      val lastValue = lastOptDoc.get("oldValue", classOf[org.bson.types.Binary]).getData
      lastOperation.set_data(lastValue)
      processEdit(lastOperation, collection)
    }
    else if (lastOperation.topic == "delete") {
      val lastValue = lastOptDoc.get("oldValue", classOf[org.bson.types.Binary]).getData
      lastOperation.set_data(lastValue)
      processAdd(lastOperation)
    }
  }
}
