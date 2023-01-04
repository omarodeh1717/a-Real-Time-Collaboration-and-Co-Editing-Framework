package org.rcs

import com.mongodb.client.model.Filters._
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, Projections}
import com.mongodb.client.{MongoClients, MongoCollection, MongoDatabase}
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession
import org.bson.Document

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

/**
 * @author ${user.name}
 */
object RealTimeEditor {
  def main(args : Array[String]) {
    // Disable warnings
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    // Configure mongodb
    println("Configure mongodb")
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/")
      .config("spark.mongodb.input.database", "kafka-records")
      .config("spark.mongodb.output.database", "kafka-records")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2")
      .getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    println("Start listening")
    val client = MongoClients.create()
    val database: MongoDatabase = client.getDatabase("kafka-records")
    val recordCollection = database.getCollection("record")
    val versionsCollection = database.getCollection("version")

    // Flush the commands every 5 minutes
    val ttl = new IndexOptions()
    ttl.expireAfter(5, TimeUnit.MINUTES)
    recordCollection.createIndex(Indexes.ascending("timestamp"), ttl)
    recordCollection.createIndex(Indexes.text("key"))

    var i = 12
    val lookbackDuration = 1000 // In milli-seconds
    var toTime = System.currentTimeMillis()
    var fromTime = toTime - lookbackDuration
    while (true) {
      try {
        val records = recordCollection.find(and(gt("timestamp", fromTime), lte("timestamp", toTime)))
        val it = records.iterator()
        if (it.hasNext) {
          val currentRecords = ArrayBuffer[Record]()
          while (it.hasNext) {
            val document = it.next()
            currentRecords.append(Utils.toRecord(document))
          }
          // Process records
          Processor.processBatch(currentRecords.sortBy(r => r.timestamp), recordCollection)
          println("Processing batch of " + currentRecords.length + " records.")
        }
        if (i == 12) {
          i = 0
          val fileNames = getAllFileNames(recordCollection)
          if (fileNames.nonEmpty) {
            Processor.saveVersion(fileNames.distinct, versionsCollection)
          }
        }
      }
      catch {
        case x: Exception => {
          println("Exception" + x)
        }
      }
      finally {
        // Set timeframe for next batch
        fromTime = toTime
        toTime = toTime + lookbackDuration
        Thread.sleep(lookbackDuration)
        i = i + 1
      }
    }
  }

  def getAllFileNames(recordCollection: MongoCollection[Document]): List[String] = {
    implicit val formats = DefaultFormats
    val projection = Projections.fields(Projections.include("key"))
    val it = recordCollection.find().projection(projection).iterator()
    if (it.hasNext) {
      val currentCommands = ArrayBuffer[Command]()
      while (it.hasNext) {
        val document = it.next()
        currentCommands.append(parse(document.getString("key")).extract[Command])
      }
      return currentCommands.map(doc => doc.session.fileName).toList
    }
    List[String]()
  }
}