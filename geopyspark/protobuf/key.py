from protobuf3.fields import Int32Field, DoubleField
from protobuf3.message import Message


class SpatialKey(Message):
    pass


class SpaceTimeKey(Message):
    pass

SpatialKey.add_field('col', Int32Field(field_number=1, optional=True))
SpatialKey.add_field('row', Int32Field(field_number=2, optional=True))
SpaceTimeKey.add_field('col', Int32Field(field_number=1, optional=True))
SpaceTimeKey.add_field('row', Int32Field(field_number=2, optional=True))
SpaceTimeKey.add_field('instant', DoubleField(field_number=3, optional=True))
