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
    string_data = app.reader.read(decoder).get('cells')
    byte_data = bytearray(string_data)
    data = np.uint8(byte_data).reshape(512, 512)

    # display tile
    size = 256, 256
    bio = io.BytesIO()
    im = Image.fromarray(data).resize(size, Image.NEAREST)
    im.save(bio, 'PNG')
    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'
    response.headers['Content-Disposition'] = 'filename=%d.png' % 0
    return response

if __name__ == "__main__":
    bucket = "azavea-datahub"
    root = "catalog"
    layer_name = "nlcd-zoomed"

    sc = SparkContext(appName="s3-flask")
    store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
    store = store_factory.buildS3(bucket, root)
    value_reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.ValueReaderFactory
    value_reader = value_reader_factory.buildS3(store)

    app.run()
