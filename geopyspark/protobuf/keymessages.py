from protobuf3.message import Message
from protobuf3.fields import Int32Field, UInt64Field


class ProtoSpatialKey(Message):
    pass


class ProtoSpaceTimeKey(Message):
    pass

ProtoSpatialKey.add_field('col', Int32Field(field_number=1, optional=True))
ProtoSpatialKey.add_field('row', Int32Field(field_number=2, optional=True))
ProtoSpaceTimeKey.add_field('col', Int32Field(field_number=1, optional=True))
ProtoSpaceTimeKey.add_field('row', Int32Field(field_number=2, optional=True))
ProtoSpaceTimeKey.add_field('instant', UInt64Field(field_number=3, optional=True))
