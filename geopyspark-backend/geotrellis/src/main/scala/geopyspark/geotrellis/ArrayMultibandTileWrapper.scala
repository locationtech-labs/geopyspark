package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.avro.codecs.Implicits._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object ArrayMultibandTileWrapper extends Wrapper[MultibandTile] {

  def testRdd(sc: SparkContext): RDD[MultibandTile] = {
    val tile = ByteArrayTile(Array[Byte](0, 0, 1, 1), 2, 2)

    val multi = Array(ArrayMultibandTile(tile, tile, tile),
      ArrayMultibandTile(tile, tile, tile),
      ArrayMultibandTile(tile, tile, tile))

    sc.parallelize(multi)
  }

  override def testIn(rdd: RDD[Array[Byte]], schema: String) = {
    val translation = PythonTranslator.fromPython[MultibandTile](rdd, Some(schema))
    val multis = translation.collect()

    multis.map(x => {
      var counter = 0
      while(counter != x.bandCount) {
        println(x.band(counter).toBytes.mkString(" "))
        counter += 1
      }
    })
  }
}
