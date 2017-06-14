package geopyspark.geotrellis

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.render._

import scala.reflect._

abstract class PngRDD[K: SpatialComponent :ClassTag] {
  def rdd: RDD[(K, Png)]
  def persist(storageLevel: StorageLevel) = rdd.persist(storageLevel)
  def unpersist() = rdd.unpersist()
}

object PngRDD {
  def asIntSingleband(tiled: SpatialTiledRasterRDD, colorMap: IntColorMap): SpatialPngRDD = {
    val rdd = tiled.rdd
    val mapped = rdd.mapValues({ mbtile =>
      mbtile.band(0).renderPng(colorMap)
    })
    new SpatialPngRDD(mapped.asInstanceOf[RDD[(tiled.keyType, Png)]])
  }

  def asSingleband(tiled: SpatialTiledRasterRDD, colorMap: DoubleColorMap): SpatialPngRDD = {
    val rdd = tiled.rdd
    val mapped = rdd.mapValues({ mbtile =>
      mbtile.band(0).renderPng(colorMap)
    })
    new SpatialPngRDD(mapped.asInstanceOf[RDD[(tiled.keyType, Png)]])
  }

  def asIntSingleband(tiled: TemporalTiledRasterRDD, colorMap: IntColorMap): TemporalPngRDD = {
    val rdd = tiled.rdd
    val mapped = rdd.mapValues({ mbtile =>
      mbtile.band(0).renderPng(colorMap)
    })
    new TemporalPngRDD(mapped.asInstanceOf[RDD[(tiled.keyType, Png)]])
  }

  def asSingleband(tiled: TemporalTiledRasterRDD, colorMap: DoubleColorMap): TemporalPngRDD = {
    val rdd = tiled.rdd
    val mapped = rdd.mapValues({ mbtile =>
      mbtile.band(0).renderPng(colorMap)
    })
    new TemporalPngRDD(mapped.asInstanceOf[RDD[(tiled.keyType, Png)]])
  }

}

class SpatialPngRDD(val rdd: RDD[(SpatialKey, Png)]) extends PngRDD[SpatialKey] {
  def lookup(col: Int, row: Int): Array[Array[Byte]] =
    rdd.lookup(SpatialKey(col, row)).map(_.bytes).toArray
}

class TemporalPngRDD(val rdd: RDD[(SpaceTimeKey, Png)]) extends PngRDD[SpaceTimeKey] {
  def lookup(col: Int, row: Int, instant: Long): Array[Array[Byte]] =
    rdd.lookup(SpaceTimeKey(col, row, instant)).map(_.bytes).toArray
}
