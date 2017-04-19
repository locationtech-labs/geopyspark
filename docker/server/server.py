import io
import numpy as np
import rasterio
import sys

from flask import Flask, make_response
from flask_cors import cross_origin
from geopyspark.geopycontext import GeoPyContext
from geopyspark.geotrellis.catalog import read_value
from geopyspark.geotrellis.constants import SPATIAL
from PIL import Image
from pyspark import SparkContext

app = Flask(__name__)
app.reader = None

def make_image(arr):
    return Image.fromarray(arr.astype('uint8')).convert('L')

def clamp(x):
    if (x < 0.0):
        x = 0
    elif (x >= 1.0):
        x = 255
    else:
        x = (int)(x * 255)
    return x

def alpha(x):
    if ((x <= 0.0) or (x > 1.0)):
        return 0
    else:
        return 255

clamp = np.vectorize(clamp)
alpha = np.vectorize(alpha)

@app.route("/geotrellis/<layer_name>/<int:x>/<int:y>/<int:zoom>.png")
@cross_origin()
def tile(layer_name, x, y, zoom):
    # fetch data
    tile = read_value(geopysc, SPATIAL, catalog, layer_name, zoom, x, y)
    arr = tile['data']

    bands = arr.shape[0]
    if bands >= 3:
        bands = 3
    else:
        bands = 1
    arrs = [np.array(arr[i, :, :]).reshape(256, 256) for i in range(bands)]

    # create tile
    if bands == 3:
        images = [make_image(clamp(arr)) for arr in arrs]
        images.append(make_image(alpha(arrs[0])))
        image = Image.merge('RGBA', images)
    else:
        gray = make_image(clamp(arrs[0]))
        alfa = make_image(alpha(arrs[0]))
        image = Image.merge('RGBA', list(gray, gray, gray, alfa))
    bio = io.BytesIO()
    image.save(bio, 'PNG')

    # return tile
    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'
    response.headers['Content-Disposition'] = 'filename=%d.png' % zoom

    return response

if __name__ == "__main__":
    if len(sys.argv) > 1:
        catalog = sys.argv[1]
    else:
        exit(-1)

    sc = SparkContext(appName = "Flask Tile Server")
    geopysc = GeoPyContext(sc)

    app.run(host='0.0.0.0', port=8033)
