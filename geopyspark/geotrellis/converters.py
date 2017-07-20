# pylint: skip-file
from py4j.java_gateway import JavaClass
from py4j.protocol import register_input_converter

from geopyspark.geotrellis import RasterizerOptions, GlobalLayout, LocalLayout, CellType
from geopyspark.geotrellis.constants import ResampleMethod


class CellTypeConverter(object):
    def can_convert(self, obj):
        return isinstance(obj, CellType)

    def convert(self, obj, gateway_client):
        JavaCellType = JavaClass("geotrellis.raster.CellType", gateway_client)
        return JavaCellType.fromName(obj.value)

      
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


class LayoutTypeConverter(object):
    def can_convert(self, object):
        return isinstance(object, GlobalLayout) or isinstance(object, LocalLayout)

    def convert(self, obj, gateway_client):
        if isinstance(obj, GlobalLayout):
            return gateway_client.jvm.geopyspark.geotrellis.GlobalLayout(obj.tile_size, obj.zoom, obj.threshold)
        elif isinstance(obj, LocalLayout):
            return gateway_client.jvm.geopyspark.geotrellis.LocalLayout(obj.tile_size)
        else:
            raise TypeError("Could not convert {} to geotrellis.raster.LayoutType".format(obj.sampleType))


class ResampleMethodConverter(object):
    def can_convert(self, object):
        return isinstance(object, ResampleMethod)

    def convert(self, obj, gateway_client):
        name = obj.value

        if name == 'NearestNeighbor':
            sample = JavaClass("geotrellis.raster.resample.NearestNeighbor$", gateway_client)
        elif name == 'Bilinear':
            sample = JavaClass("geotrellis.raster.resample.Bilinear$", gateway_client)
        elif name == 'CubicConvolution':
            sample = JavaClass("geotrellis.raster.resample.CubicConvolution$", gateway_client)
        elif name == 'CubicSpline':
            sample = JavaClass("geotrellis.raster.resample.CubicSpline$", gateway_client)
        elif name == 'Lanczos':
            sample = JavaClass("geotrellis.raster.resample.Lanczos$", gateway_client)
        elif name == 'Average':
            sample = JavaClass("geotrellis.raster.resample.Average$", gateway_client)
        elif name == 'Mode':
            sample = JavaClass("geotrellis.raster.resample.Mode$", gateway_client)
        elif name == 'Median':
            sample = JavaClass("geotrellis.raster.resample.Median$", gateway_client)
        elif name == 'Max':
            sample = JavaClass("geotrellis.raster.resample.Max$", gateway_client)
        elif name == 'Min':
            sample = JavaClass("geotrellis.raster.resample.Min$", gateway_client)
        else:
            raise TypeError(name, "Could not be converted to a GeoTrellis ResampleMethod.")

        return sample.__getattr__("MODULE$")


register_input_converter(CellTypeConverter(), prepend=True)
register_input_converter(RasterizerOptionsConverter(), prepend=True)
register_input_converter(LayoutTypeConverter(), prepend=True)
register_input_converter(ResampleMethodConverter(), prepend=True)
