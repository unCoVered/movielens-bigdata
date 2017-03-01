package com.uncovered.spark.partitioner

import org.apache.spark.Partitioner

class MovieKeyPartitioner (val numParts: Int, val range: Int) extends Partitioner {

  def getPartition(key: Any): Int = 0

  def numPartitions =  this.numParts
}
