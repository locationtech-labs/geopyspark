# pylint: skip-file
from py4j.java_gateway import JavaClass
from py4j.protocol import register_input_converter

from geopyspark.geotrellis import RasterizerOptions

class RasterizerOptionsConverter(object):
    def can_convert(self, object):
        return isinstance(object, RasterizerOptions)

    def convert(self, object, gateway_client):
        JavaRasterizerOptions = JavaClass("geotrellis.raster.rasterize.Rasterizer$Options$", gateway_client)
        if (object.sampleType == 'PixelIsPoint'):
            sample = JavaClass("geotrellis.raster.PixelIsPoint$", gateway_client)
        elif (object.sampleType == 'PixelIsArea'):
            sample = JavaClass("geotrellis.raster.PixelIsArea$", gateway_client)
        else:
            raise TypeError("Could not convert {} to geotrellis.raster.PixelSampleType".format(object.sampleType))

        sample_instance = sample.__getattr__("MODULE$")
        return JavaRasterizerOptions().apply(object.includePartial, sample_instance)


register_input_converter(RasterizerOptionsConverter(), prepend=True)
