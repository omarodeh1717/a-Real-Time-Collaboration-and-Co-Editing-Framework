package org.rcs.framework

import net.liftweb.json._
import net.liftweb.json.Serialization.write

class RCSSession (config: RCSConfig){
  RCSClient.start()
  implicit val formats = DefaultFormats

  def add(userId: String, data: Either[String, Array[Byte]], startPosition: Int): Unit = {
    val bytes = data.fold(a => a.getBytes(), y => y)
    RCSClient.sendRecord("add", write(new RecordConfig(userId, config, startPosition)), bytes)
  }

  def edit(userId: String, data: Either[String, Array[Byte]], startPosition: Int): Unit = {
    val bytes = data.fold(a => a.getBytes(), y => y)
    RCSClient.sendRecord("edit", write(new RecordConfig(userId, config, startPosition)), bytes)
  }

  def delete(userId: String, startPosition: Int, endPosition: Int): Unit = {
    RCSClient.sendRecord("delete", write(new RecordConfig(userId, config, startPosition, endPosition)), Array())
  }

  def undo(userId: String): Unit = {
    RCSClient.sendRecord("undo", write(new RecordConfig(userId, config)), Array())
  }

  def endSession(): Unit = {
    implicit val formats = DefaultFormats
    RCSClient.sendRecord("session", "end", config.fileName.getBytes())
  }

}
