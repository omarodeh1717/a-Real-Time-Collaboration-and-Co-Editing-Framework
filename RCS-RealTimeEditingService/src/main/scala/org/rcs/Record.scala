package org.rcs

import org.bson.types.ObjectId

class RCSConfig (val _fileName: String,
                 val _sessionId: String = java.util.UUID.randomUUID.toString)
{
  def fileName: String = _fileName
  def sessionId: String = _sessionId
}

class Command (val _userId: String,
                    val _session: RCSConfig,
                    val _startPosition: Int,
                    val _endPosition: Int)
{
  def userId: String = _userId
  def session: RCSConfig = _session
  def startPosition: Int = _startPosition
  def endPosition: Int = _endPosition
}


class Record  (val _id: ObjectId,
               val _topic: String,
               val _timestamp: Long,
               val _command: Command,
               var _data: Array[Byte])
{
  def id: ObjectId = _id
  def topic: String = _topic
  def timestamp: Long = _timestamp
  def command: Command = _command
  def data: Array[Byte] = _data

  def set_data(data: Array[Byte]) = {
    _data = data
  }
  def text() :String = new String(data)
}
