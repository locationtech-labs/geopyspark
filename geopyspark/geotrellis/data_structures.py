from collections import namedtuple


Extent = namedtuple("Extent", 'xmin ymin xmax ymax')
TileLayout = namedtuple("TileLayout", 'layoutCols layoutRows tileCols tileRows')
LayoutDefinition = namedtuple("LayoutDefinition", 'extent tileLayout')
Bounds = namedtuple("Bounds", 'minKey maxKey')


class Metadata(object):
    def __init__(self, metadata_dict):
        self.metadata_dict = metadata_dict
        self.crs = self.metadata_dict['crs']
        self.cell_type = self.metadata_dict['cellType']

        self.bounds = Bounds(**self.metadata_dict['bounds'])
        self.extent = Extent(**self.metadata_dict['extent'])
        self.tile_layout = TileLayout(**self.metadata_dict['layoutDefinition']['tileLayout'])

        self.layout_definition = LayoutDefinition(
            Extent(**self.metadata_dict['layoutDefinition']['extent']), self.tile_layout)
