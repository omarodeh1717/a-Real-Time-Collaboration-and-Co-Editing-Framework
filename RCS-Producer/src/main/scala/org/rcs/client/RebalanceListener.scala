package org.rcs.client

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import java.util

class RebalanceListener extends ConsumerRebalanceListener {
  override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
    print("Assigned Partitions:")
    val it = collection.iterator()
    while (it.hasNext) {
      print(it.next().partition() + ",")
    }
    println
  }

  override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
    print("Revoked Partitions:")
    val it = collection.iterator()
    while (it.hasNext) {
      print(it.next().partition() + ",")
    }
    println
  }
}