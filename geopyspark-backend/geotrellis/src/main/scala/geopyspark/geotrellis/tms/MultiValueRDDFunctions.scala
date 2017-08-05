package geopyspark.geotrellis.tms

import org.apache.spark._
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import scala.reflect._

class MultiValueRDDFunctions[K, V](self: RDD[(K, V)])
  (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
    extends Serializable {

  /**
   * Return the list of values in the RDD for each key.
   * This operation is done efficiently if the RDD has a known partitioner
   * by only searching the partitions that the keys map to.
   */
  def multilookup(keys: Set[K]): Seq[(K,V)] =  {
    self.partitioner match {
      case Some(p) =>
        val indecies = keys.map(p.getPartition(_)).toSeq
        val process = (it: Iterator[(K, V)]) => {
          val buf = new ArrayBuffer[(K, V)]
          for (pair <- it if keys contains pair._1) {
            buf += pair
          }
          buf
        } : Seq[(K, V)]
        val res = self.context.runJob(self, process, indecies)
        res.toSeq.flatten
      case None =>
        self.filter{pair => keys.contains(pair._1) }.collect()
    }
  }

}
