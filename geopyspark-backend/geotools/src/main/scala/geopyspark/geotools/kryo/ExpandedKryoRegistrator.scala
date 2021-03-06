package geopyspark.geotools.kryo

import geotrellis.spark.io.kryo._

import com.esotericsoftware.kryo.Kryo
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.feature.simple.SimpleFeatureImpl

import de.javakaffee.kryoserializers._


class ExpandedKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) = {
    UnmodifiableCollectionsSerializer.registerSerializers(kryo)
    kryo.register(classOf[SimpleFeature])
    kryo.register(classOf[SimpleFeatureImpl])
    kryo.register(classOf[SimpleFeatureType])
    super.registerClasses(kryo)
  }
}
