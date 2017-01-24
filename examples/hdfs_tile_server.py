import avro
import avro.io
import io
import numpy as np
import sys

from flask import Flask, make_response
from geopyspark.avroserializer import AvroSerializer
from PIL import Image
from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer


app = Flask(__name__)
app.reader = None

@app.route("/<int:zoom>/<int:x>/<int:y>.png")
def tile(x, y, zoom):
    # fetch tile
    tup = value_reader.readSpatialSingleband(layer_name, zoom, x, y)
    (serialized_tile, schema) = (tup._1(), tup._2())
    if app.reader == None:
        app.reader = avro.io.DatumReader(avro.schema.Parse(schema))
    tile_buffer = io.BytesIO(serialized_tile)
    decoder = avro.io.BinaryDecoder(tile_buffer)
    data = np.uint8(app.reader.read(decoder).get('cells')).reshape(256, 256)

    # display tile
    bio = io.BytesIO()
    im = Image.fromarray(data)
    im.save(bio, 'PNG')
    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'
    response.headers['Content-Disposition'] = 'filename=%d.png' % 0
    return response

if __name__ == "__main__":
    if len(sys.argv) > 2:
        uri = sys.argv[1]
        layer_name = sys.argv[2]
    else:
        exit(-1)

    # bootstrap, boilerplate
    sc = SparkContext(appName="hdfs-flask")
    store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
    store = store_factory.buildHadoop(uri, sc._jsc.sc())
    value_reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.ValueReaderFactory
    value_reader = value_reader_factory.buildHadoop(store)

    app.run()
