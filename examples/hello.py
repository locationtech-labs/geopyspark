import numpy as np
import io

from flask import Flask, send_file, make_response
from geopyspark.avroserializer import AvroSerializer
from PIL import Image


app = Flask(__name__)

@app.route("/")
def hello():
    bio = io.BytesIO()
    im = Image.fromarray(np.uint8(np.zeros(256*256).reshape(256,256)))
    im.save(bio, 'PNG')
    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'
    response.headers['Content-Disposition'] = 'filename=%d.png' % 0
    return response

if __name__ == "__main__":
    app.run()
