from PIL import Image
import numpy as np

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
        tile = np.array(list(cells)) # turn tile to array with bands
        image=render_function(tile)
        bio = io.BytesIO()
        image.save(bio, 'PNG')
        return bio.getvalue()


    class Java:
        implements = ["geopyspark.geotrellis.tms.TileRender"]


from geonotebook.vis.geotrellis.render_methods import render_nlcd

def make_tms(geopysc):
    tr = TileRender(render_nlcd)
    server = geopysc._jvm.geopyspark.geotrellis.tms.Server.getItHere(tr)
    return server
    # make a server
    # give it NLCD function ... from geonotebook ?
