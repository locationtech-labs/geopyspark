import io
import numpy as np

from geopyspark.geotrellis.layer import Pyramid


class TileRender(object):
    """A Python implementation of the Scala geopyspark.geotrellis.tms.TileRender
    interface.  Permits a callback from Scala to Python to allow for custom
    rendering functions.

    Args:
        render_function (numpy.ndarray => bytes): A function to convert a numpy
            array to a collection of bytes giving a binary image file.

    Attributes:
        render_function (numpy.ndarray => bytes): A function to convert a numpy
            array to a collection of bytes giving a binary image file.
    """

    def __init__(self, render_function):
        # render_function: numpyarry => Image
        self.render_function = render_function

    def render(self, cells, cols, rows): # return `bytes`
        """A function to convert an array to an image.

        Args:
            cells (bytes): A linear array of bytes representing the contents of
                a tile
            rows (int): The number of rows in the final array
            cols (int): The number of cols in the final array

        Returns:
            bytes representing an image
        """
        try:
            # tile = np.array(list(cells)) # turn tile to array with bands
            print("Reshaping to {}x{} matrix".format(rows, cols))
            tile = np.reshape(np.frombuffer(cells, dtype="uint8"), (1, rows, cols)) # turn tile to array with bands
            print("Rendering tile")
            image = self.render_function(tile)
            print("Saving result")
            bio = io.BytesIO()
            image.save(bio, 'PNG')
            return bio.getvalue()
        except Exception:
            from traceback import print_exc
            print_exc()

    class Java(object):
        implements = ["geopyspark.geotrellis.tms.TileRender"]


class TMSServer(object):
    """Represents a TMS server. The server begins running at initialization."""

    def __init__(self, pysc, server):
        self.pysc = pysc
        self.server = server
        self.handshake = ''
        self.pysc._gateway.start_callback_server()

    def set_handshake(self, handshake):
        self.server.set_handshake(handshake)
        self.handshake = handshake

    @classmethod
    def s3_catalog_tms_server(cls, pysc, bucket, root, catalog, color_map):
        """A function to create a ``TMSServer`` for a catalog stored in an S3 bucket.

        Args:
            bucket (string): The name of the S3 bucket
            root (string): The key in the bucket containing the catalog
            catalog (string): The name of the catalog
            color_map (ColorMap): A ColorMap to use in rendering the catalog tiles

        Returns:
            :class:`~geopyspark.geotrellis.tms.TMSServer`
        """

        server = pysc._gateway.jvm.geopyspark.geotrellis.tms.TMSServer.serveS3Catalog(bucket, root, catalog, color_map.cmap)
        return cls(pysc, server)

    @classmethod
    def remote_tms_server(cls, pysc, pattern_url):
        """A function to create a TMS server delivering tiles from a remote TMS server

        Args:
            pattern_url (string): A string giving the form of the URL where tiles
                are stored.  The pattern should contain the literals '{z}', '{x}',
                and '{y}' giving the zoom, x, and y keys of the desired tile,
                respectively.

        Returns:
            :class:`~geopyspark.geotrellis.tms.TMSServer`
        """

        server = pysc._gateway.jvm.geopyspark.geotrellis.tms.TMSServer.serveRemoteTMSLayer(pattern_url)
        return cls(pysc, server)

    @classmethod
    def rdd_tms_server(cls, pysc, pyramid, color_map):
        """Creates a ``TMSServer`` from a ``Pyramid`` instance.

        Args:
            pyramid (list or `~geopyspark.geotrellis.layer.Pyramid`): Either a list of pyramided
                ``TiledRasterLayer``\s or a ``Pyramid`` instance.
            color_map (`~geopyspark.geotrellis.color.ColorMap`): A ``ColorMap`` instance used to color
                the resulting tiles.

        Returns:
            :class:`~geopyspark.geotrellis.tms.TMSServer`
        """

        if isinstance(pyramid, list):
            pyramid = Pyramid(pyramid)
        rdd_levels = {k: v.srdd.rdd() for k, v in pyramid.levels.items()}
        server = pysc._gateway.jvm.geopyspark.geotrellis.tms.TMSServer.serveSpatialRdd(rdd_levels, color_map.cmap, 0)
        return cls(pysc, server)
