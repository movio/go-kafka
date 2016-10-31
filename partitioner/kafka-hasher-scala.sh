#!/bin/sh
exec scala -cp \
slf4j-log4j12-1.7.21.jar:\
kafka-clients-0.10.1.0.jar:\
slf4j-api-1.7.21.jar:\
log4j-1.2.17.jar:\
mockito-core-2.2.9.jar:\
byte-buddy-1.5.0.jar:\
byte-buddy-agent-1.5.0.jar:\
objenesis-2.4.jar:\
. \
     $0 $@
!#

import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.mockito.Mockito._
import scala.collection.JavaConversions._

// See https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java
object KafkaHasherTest extends App {
    val key = "c03a3475-3ed6-4ed1-8ae5-1c432da43e73"
  val numPartitions = 100

  val p = new DefaultPartitioner()
  val cluster = mock(classOf[Cluster])
  val l: java.util.List[PartitionInfo] = List.fill[PartitionInfo](100)(null)
  when(cluster.partitionsForTopic("topic")).thenReturn(l);

  // Results
  println(s"""{[]byte("$key"), ${Utils.toPositive(Utils.murmur2(key.getBytes))}, ${partition(key.getBytes, 2)}, ${partition(key.getBytes, 3)}, ${partition(key.getBytes, 100)}, ${partition(key.getBytes, 999)}},""")

  def partition(keyBytes: Array[Byte], partitions: Int) = {
    val p = new DefaultPartitioner()
    val cluster = mock(classOf[Cluster])
    val l: java.util.List[PartitionInfo] = List.fill[PartitionInfo](partitions)(null)
    when(cluster.partitionsForTopic("topic")).thenReturn(l);
    p.partition("topic", null, keyBytes, null, null, cluster)
  }
}

KafkaHasherTest.main(args)
