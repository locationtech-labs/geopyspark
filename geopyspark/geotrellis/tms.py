from PIL import Image
import numpy as np
import io

from py4j.clientserver import ClientServer, JavaParameters, PythonParameters

# Return URL of tile server for a layer
## http://localhost:2342/s3/azavea-datahub/catalog/tms/{z}/{x}/{y}
def layer_tms(rdd_type, uri, layer_name, render_function=None, cmap=None):
    # we neeed the setup so we can report common errors
    pass

def use_case():
    url = layer_tms(SPATIAL, "s3://azavea-datahub/catalog", "us-ned-tms-epsg3857", cmap="viridean")

class TileRender(object):

    def __init__(self, render_function):
        # render_function: numpyarry => Image
        self.render_function = render_function

    def render(self, cells, cols, rows): # return `bytes`
        try:
            # tile = np.array(list(cells)) # turn tile to array with bands
            print("Reshaping to {}x{} matrix".format(rows, cols))
            tile = np.reshape(np.frombuffer(cells, dtype="uint8"), (1, rows, cols)) # turn tile to array with bands
            print("Rendering tile")
            image=self.render_function(tile)
            print("Saving result")
            bio = io.BytesIO()
            image.save(bio, 'PNG')
            return bio.getvalue()
        except Exception:
            from traceback import print_exc
            print_exc()

    class Java:
        implements = ["geopyspark.geotrellis.tms.TileRender"]

class TMSServer(object):
    def __init__(self, geopysc, server):
        self.geopysc = geopysc
        self.server = server
        self.handshake = ''

    def set_handshake(self, handshake):
        self.server.set_handshake(handshake)
        self.handshake = handshake

def make_s3_tms(geopysc, bucket, root, catalog, colormap):
    server = geopysc._jvm.geopyspark.geotrellis.tms.TMSServer.serveS3Catalog(bucket, root, catalog, colormap.cmap)
    return TMSServer(geopysc, server)

def remote_tms_server(geopysc, pattern_url):
    server = geopysc._jvm.geopyspark.geotrellis.tms.TMSServer.serveRemoteTMSLayer(pattern_url)
    return TMSServer(geopysc, server)
