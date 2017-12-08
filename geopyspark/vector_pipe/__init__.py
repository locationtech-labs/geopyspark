from collections import namedtuple


Feature = namedtuple("Feature", "geometry properties")

Properties = namedtuple("Properties", "element_id user uid changeset version minor_version timestamp visible tags")

CellValue = namedtuple("CellValue", "value zindex")


__all__ = ['Feature', 'Properties', 'CellValue']

from . import osm_reader

__all__ += ['osm_reader']
