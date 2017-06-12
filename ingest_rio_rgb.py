from geopyspark.geopycontext import GeoPyContext
from geopyspark.geotrellis.constants import SPATIAL, ZOOM
from geopyspark.geotrellis.catalog import write
from geopyspark.geotrellis.geotiff_rdd import get

geopysc = GeoPyContext(appName="python-ingest", master="local[*]")

catalog_uri="/Users/yoninachmany/azavea/geopyspark/docker/notebooks/spacenet-dataset/AOI_1_Rio/"
# Compressed 3band 200m x 200m tiles with associated building foot print labels
processed_uri=catalog_uri+"processedData/processedBuildingLabels/3band/"
# 3band (RGB) Raster Mosaic for Rio De Jenairo area (2784 sq KM) collected by WorldView-2
src_uri=catalog_uri+"srcData/rasterData/3-Band/"

def ingest(uri, processed_or_src):
    rdd = get(geopysc, SPATIAL, uri, maxTileSize=256, numPartitions=100)

    metadata = rdd.collect_metadata()

    # tile the rdd to the layout defined in the metadata
    laid_out = rdd.tile_to_layout(metadata).cache().repartition(150)

    # reproject the tiled rasters using a ZoomedLayoutScheme
    reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)

    # pyramid the TiledRasterRDD to create 12 new TiledRasterRDDs
    # one for each zoom level
    pyramided = reprojected.pyramid(start_zoom=12, end_zoom=1)

    # Save each TiledRasterRDDs locally
    for tiled in pyramided:
        write("file:///tmp/%s-catalog" % processed_or_src, "%s-ingest" % processed_or_src, tiled)

ingest(processed_uri, "processed")
ingest(src_uri, "src")
