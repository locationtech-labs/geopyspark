from protobuf3.fields import MessageField, DoubleField, Int32Field, StringField
from protobuf3.message import Message


class CRS(Message):
    pass


class Extent(Message):
    pass


class ProjectedExtent(Message):
    pass


class TemporalProjectedExtent(Message):
    pass

CRS.add_field('epsg', Int32Field(field_number=1, optional=True))
CRS.add_field('proj4', StringField(field_number=2, optional=True))
Extent.add_field('xmin', DoubleField(field_number=1, optional=True))
Extent.add_field('ymin', DoubleField(field_number=2, optional=True))
Extent.add_field('xmax', DoubleField(field_number=3, optional=True))
Extent.add_field('ymax', DoubleField(field_number=4, optional=True))
ProjectedExtent.add_field('extent', MessageField(field_number=1, optional=True, message_cls=Extent))
ProjectedExtent.add_field('crs', MessageField(field_number=2, optional=True, message_cls=CRS))
TemporalProjectedExtent.add_field('extent', MessageField(field_number=1, optional=True, message_cls=Extent))
TemporalProjectedExtent.add_field('crs', MessageField(field_number=2, optional=True, message_cls=CRS))
TemporalProjectedExtent.add_field('instant', DoubleField(field_number=3, optional=True))
