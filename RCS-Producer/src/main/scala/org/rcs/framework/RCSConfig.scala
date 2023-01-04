package org.rcs.framework

class RCSConfig (val _fileName: String,
                 val _sessionId: String = java.util.UUID.randomUUID.toString)
{
  def fileName: String = _fileName
  def sessionId: String = _sessionId
}

class RecordConfig (val _userId: String,
                    val _session: RCSConfig,
                    val _startPosition: Int = 0,
                    val _endPosition: Int = 0)
{
  def userId: String = _userId
  def sessionId: RCSConfig = _session
  def startPosition: Int = _startPosition
  def endPosition: Int = _endPosition
}
