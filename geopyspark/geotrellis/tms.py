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

from geonotebook.vis.geotrellis.render_methods import render_nlcd

def make_s3_tms(geopysc, handshake, bucket, root, catalog, colormap):
    print("Creating Scala tile server")
    #tr = TileRender(render_nlcd)
    server = geopysc._jvm.geopyspark.geotrellis.tms.Server.serveS3Catalog(handshake, bucket, root, catalog, colormap.cmap)
    # gateway = ClientServer(
    #     java_parameters = JavaParameters(),
    #     python_parameters = PythonParameters())#,
    #     #python_server_entry_point = tr)
    return server
    # make a server
    # give it NLCD function ... from geonotebook ?

