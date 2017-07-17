import io
import numpy as np

from geopyspark.geotrellis.color import ColorMap
from geopyspark.geotrellis.layer import Pyramid
from geopyspark.geotrellis.protobufcodecs import multibandtile_decoder

__all__ = ['TileRender', 'TMSServer']


class TileRender(object):
    """A Python implementation of the Scala geopyspark.geotrellis.tms.TileRender
    interface.  Permits a callback from Scala to Python to allow for custom
    rendering functions.

    Attributes:
        render_function (numpy.ndarray => bytes): A function to convert a numpy
            array to a collection of bytes giving a binary image file.
    """

    def __init__(self, render_function):
        """Default constructor.

        Args:
            render_function (np.array => bytes): A function to convert a numpy 
                array to a collection of bytes giving a binary image file.

        Returns:
            [TileRender]
        """
        self.render_function = render_function

    def requiresEncoding(self):
        return True

    def renderEncoded(self, scala_array):
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
            tile = multibandtile_decoder(scala_array)
            cells = tile.cells
            image = self.render_function(cells)
            bio = io.BytesIO()
            image.save(bio, 'PNG')
            return bio.getvalue()
        except Exception:
            from traceback import print_exc
            print_exc()

    class Java:
        implements = ["geopyspark.geotrellis.tms.TileRender"]

class TileCompositer(object):
    """A Python implementation of the Scala geopyspark.geotrellis.tms.TileCompositer
    interface.  Permits a callback from Scala to Python to allow for custom
    compositing functions.
    """

    def __init__(self, composite_function):
        """Default constructor.

        Args:
            render_function (list[numpy array] => bytes): A function to convert
                a list of numpy arrays to a collection of bytes giving a binary
                image file.

        Returns:
            [TileCompositer]
        """
        # composite_function: List[np.array] => Image
        self.composite_function = composite_function

    def requiresEncoding(self):
        return True

    def compositeEncoded(self, all_scala_arrays): # return `bytes`
        """A function to convert an array to an image.

        Args:
            all_scala_arrays (array of bytes): An array containing the encoded
                representations of the incoming tiles

        Returns:
            [bytes] representing an image
        """
        try:
            cells = [multibandtile_decoder(scala_array).cells for scala_array in all_scala_arrays]
            image = self.composite_function(cells)
            bio = io.BytesIO()
            image.save(bio, 'PNG')
            return bio.getvalue()
        except Exception:
            from traceback import print_exc
            print_exc()

    class Java:
        implements = ["geopyspark.geotrellis.tms.TileCompositer"]

class TMS(object):
    """Provides a TMS server for raster data.

    In order to display raster data on a variety of different map interfaces
    (e.g., leaflet maps, geojson.io, GeoNotebook, and others), we provide
    the TMS class.
    """
    def __init__(self, pysc, server):
        self.pysc = pysc
        self.server = server
        self.handshake = ''
        pysc._gateway.start_callback_server()

    def set_handshake(self, handshake):
        self.server.set_handshake(handshake)
        self.handshake = handshake

    @classmethod
    def build(cls, pysc, *args, **kwargs):
        """Builds a TMS server from one or more layers.

        This function takes a SparkContext, a list of sources, and a display
        method and creates a TMS server to display the desired content.  The
        sources are supplied as a comma-separated list following the
        SparkContext where URIs may be given for catalog sources, and Pyramid
        objects may be passed for RDD-based sources.  The URIs currently
        supported are formatted as 's3://path/to/catalog'.

        It will also be necessary to provide a means to display the tile inputs.

        Args:
            pysc (SparkContext): The Spark context

            One or more unnamed arguments of type
                Pyramid: A pyramided RDD
                (string, string): The URI of the catalog paired with the layer
                    name; i.e., ("s3://bucket/root", "layer_name")

            Exactly one of the following keyword arguments:
                render (ColorMap, callable): A color map or function from
                    np.array to PIL.Image object that will be used for display.
                    Takes only the first supplied tile source as input
                composite (callable): A function from a list of np.array to
                    PIL.Image
        """
        def makeReader(arg):
            if isinstance(arg, Pyramid):
                reader = pysc._gateway.jvm.geopyspark.geotrellis.tms.TileReaders.createSpatialRddReader(arg.levels, pysc._gateway.jvm.geopyspark.geotrellis.tms.AkkaSystem.system)
            elif isinstance(arg, tuple) and isinstance(arg[0], str) and isinstance(arg[1], str):
                reader = pysc._gateway.jvm.geopyspark.geotrellis.tms.TileReaders.createCatalogReader(arg[0], arg[1])
            else:
                raise ValueError('Arguments must be of type Pyramid or (string, string)')

            return reader

        if 'render' in kwargs and 'composite' not in kwargs:
            renderer = kwargs['render']
            if callable(renderer):
                display = TileRender(renderer)
            elif isinstance(renderer, ColorMap):
                display = pysc._jvm.geopyspark.geotrellis.tms.RenderSinglebandFromCM.apply(renderer.cmap)
            else:
                raise ValueError("'render' keyword argument must either be a function or a ColorMap")
            reader = makeReader(args[0])
            route = pysc._jvm.geopyspark.geotrellis.tms.TMSServerRoutes.renderingTileRoute(reader, display)
        elif 'render' not in kwargs and 'composite' in kwargs:
            composite = kwargs['composite']
            if not callable(composite):
                raise ValueError("'composite' keyword argument must be a function!")
            readers = [makeReader(arg) for arg in args]
            route = pysc._jvm.geopyspark.geotrellis.tms.TMSServerRoutes.compositingTileRoute(readers, TileCompositer(composite))
        else:
            raise ValueError("Must specify exactly one of the following keyword parameters: 'render' or 'composite'")

        server = pysc._jvm.geopyspark.geotrellis.tms.TMSServer.createServer(route)
        return cls(pysc, server)
