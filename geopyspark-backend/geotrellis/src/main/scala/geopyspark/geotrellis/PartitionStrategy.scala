package geopyspark.geotrellis

import org.apache.spark._


abstract class PartitionStrategy(numPartitions: Option[Int]) {
  def producePartitioner(partitions: Int): Option[Partitioner]
}


class HashPartitionStrategy(val numPartitions: Option[Int]) extends PartitionStrategy(numPartitions) {
  def producePartitioner(partitions: Int): Option[Partitioner] =
      numPartitions match {
      case None => Some(new HashPartitioner(partitions))
      case Some(num) => Some(new HashPartitioner(num))
    }
}

object HashPartitionStrategy {
  def apply(numPartitions: Integer): HashPartitionStrategy =
    numPartitions match {
      case i: Integer => new HashPartitionStrategy(Some(i.toInt))
      case null => new HashPartitionStrategy(None)
    }
}


class SpatialPartitionStrategy(val numPartitions: Option[Int], val bits: Int) extends PartitionStrategy(numPartitions) {
  def producePartitioner(partitions: Int): Option[Partitioner] =
    numPartitions match {
      case None => Some(SpatialPartitioner(partitions, bits))
      case Some(num) => Some(SpatialPartitioner(num, bits))
    }
}

object SpatialPartitionStrategy {
  def apply(numPartitions: Integer, bits: Int): SpatialPartitionStrategy =
    numPartitions match {
      case i: Integer => new SpatialPartitionStrategy(Some(i.toInt), bits)
      case null => new SpatialPartitionStrategy(None, bits)
    }
}


class SpaceTimePartitionStrategy(
  val numPartitions: Option[Int],
  val bits: Int,
  val temporalType: String,
  val temporalResolution: String
) extends PartitionStrategy(numPartitions) {
  def producePartitioner(partitions: Int): Option[Partitioner] =
    numPartitions match {
      case None => Some(SpaceTimePartitioner(partitions, bits, temporalType, temporalResolution))
      case Some(num) => Some(SpaceTimePartitioner(num, bits, temporalType, temporalResolution))
    }
}

object SpaceTimePartitionStrategy {
  def apply(
    numPartitions: Integer,
    bits: Int,
    temporalType: String,
    temporalResolution: String
  ): SpaceTimePartitionStrategy =
    numPartitions match {
      case i: Integer => new SpaceTimePartitionStrategy(Some(i.toInt), bits, temporalType, temporalResolution)
      case null => new SpaceTimePartitionStrategy(None, bits, temporalType, temporalResolution)
    }
}
