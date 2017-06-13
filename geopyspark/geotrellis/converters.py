from py4j.java_gateway import JavaObject, JavaMember, get_method, JavaClass
from py4j.protocol import (
    Py4JError, get_command_part, get_return_value, register_input_converter,
    register_output_converter)

from geopyspark.geotrellis import RasterizeOptions

class RasterizeOptionsConverter(object):
    def can_convert(self, object):
        return isinstance(object, RasterizeOptions)

    def convert(self, object, gateway_client):
        JavaRasterizeOptions = JavaClass("geotrellis.raster.rasterize.Rasterizer$Options$", gateway_client)
        if (object.sampleType == 'PixelIsPoint'):
            sample = JavaClass("geotrellis.raster.PixelIsPoint$", gateway_client)
        elif (object.sampleType == 'PixelIsArea'):
            sample = JavaClass("geotrellis.raster.PixelIsArea$", gateway_client)
        else:
            raise TypeError("Could not convert {} to geotrellis.raster.PixelSampleType".format(object.sampleType))

        sample_instance = sample.__getattr__("MODULE$")
        return JavaRasterizeOptions().apply(object.includePartial, sample_instance)


register_input_converter(RasterizeOptionsConverter(), prepend=True)
