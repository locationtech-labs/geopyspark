from tileMessages import ProtoMultibandTile
from extentMessages import ProtoTemporalProjectedExtent, ProtoProjectedExtent
from keyMessages import ProtoSpatialKey, ProtoSpaceTimeKey
from protobuf3.message import Message
from protobuf3.fields import MessageField


class ProtoTuple(Message):
    pass

ProtoTuple.add_field('projectedExtent', MessageField(field_number=1, optional=True, message_cls=ProtoProjectedExtent))
ProtoTuple.add_field('temporalProjectedExtent', MessageField(field_number=2, optional=True, message_cls=ProtoTemporalProjectedExtent))
ProtoTuple.add_field('spatialKey', MessageField(field_number=3, optional=True, message_cls=ProtoSpatialKey))
ProtoTuple.add_field('spaceTimeKey', MessageField(field_number=4, optional=True, message_cls=ProtoSpaceTimeKey))
ProtoTuple.add_field('tiles', MessageField(field_number=5, optional=True, message_cls=ProtoMultibandTile))
