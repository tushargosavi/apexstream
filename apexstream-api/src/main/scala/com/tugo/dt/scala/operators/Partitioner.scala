package com.tugo.dt.scala.operators

import java.util

import com.datatorrent.api.DefaultPartition
import com.datatorrent.api.Operator.InputPort
import com.datatorrent.api.Partitioner.{Partition, PartitioningContext}
import com.datatorrent.common.partitioner.StatelessPartitioner
import com.datatorrent.lib.codec.KryoSerializableStreamCodec
import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.serializers.JavaSerializer

/** partitioner which takes list of ports and partitions data on those ports */
@DefaultSerializer(classOf[JavaSerializer])
class Partitioner[T](ports : Seq[InputPort[_]]) extends StatelessPartitioner[T] {
  override def definePartitions(partitions: util.Collection[Partition[T]], context: PartitioningContext): util.Collection[Partition[T]] = {
    val parts = super.definePartitions(partitions, context)
    for (port <- ports) {
      DefaultPartition.assignPartitionKeys(parts, port)
    }
    parts
  }
}

/** return a streamcodec from a function which takes and object and returns an
  * hashCode.
  * @param func
  * @tparam T
  */
@DefaultSerializer(classOf[JavaSerializer])
class StreamCodec[T](func : T => Int) extends KryoSerializableStreamCodec[T] {
  override def getPartition(t: T): Int = func(t)
}
