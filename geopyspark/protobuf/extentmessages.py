from protobuf3.message import Message
from protobuf3.fields import MessageField, Int32Field, DoubleField, StringField, UInt64Field


class ProtoCRS(Message):
    pass


class ProtoExtent(Message):
    pass


class ProtoProjectedExtent(Message):
    pass


class ProtoTemporalProjectedExtent(Message):
    pass

ProtoCRS.add_field('epsg', Int32Field(field_number=1, optional=True))
ProtoCRS.add_field('proj4', StringField(field_number=2, optional=True))
ProtoExtent.add_field('xmin', DoubleField(field_number=1, optional=True))
ProtoExtent.add_field('ymin', DoubleField(field_number=2, optional=True))
ProtoExtent.add_field('xmax', DoubleField(field_number=3, optional=True))
ProtoExtent.add_field('ymax', DoubleField(field_number=4, optional=True))
ProtoProjectedExtent.add_field('extent', MessageField(field_number=1, optional=True, message_cls=ProtoExtent))
ProtoProjectedExtent.add_field('crs', MessageField(field_number=2, optional=True, message_cls=ProtoCRS))
ProtoTemporalProjectedExtent.add_field('extent', MessageField(field_number=1, optional=True, message_cls=ProtoExtent))
ProtoTemporalProjectedExtent.add_field('crs', MessageField(field_number=2, optional=True, message_cls=ProtoCRS))
ProtoTemporalProjectedExtent.add_field('instant', UInt64Field(field_number=3, optional=True))
