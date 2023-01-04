package org.rcs

import net.liftweb.json.{DefaultFormats, parse}
import org.bson.Document

object Utils {
  implicit val formats = DefaultFormats
  def toRecord(document: Document): Record = {
    new Record(
      document.getObjectId("_id"),
      document.getString("topic"),
      document.getLong("timestamp"),
      parse(document.getString("key")).extract[Command],
      document.get("value", classOf[org.bson.types.Binary]).getData()
    )
  }
}
