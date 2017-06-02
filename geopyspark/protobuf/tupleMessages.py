from protobuf3.fields import MessageField
from extentMessages import TemporalProjectedExtent, ProjectedExtent
from tileMessages import MultibandTile
from keyMessages import SpatialKey, SpaceTimeKey
from protobuf3.message import Message


class First(Message):
    pass


class Tuple2(Message):
    pass

First.add_field('projected_extnet', MessageField(field_number=1, optional=True, message_cls=ProjectedExtent))
First.add_field('temporal_projected_extnet', MessageField(field_number=2, optional=True, message_cls=TemporalProjectedExtent))
First.add_field('spatial_key', MessageField(field_number=3, optional=True, message_cls=SpatialKey))
First.add_field('space_time_key', MessageField(field_number=4, optional=True, message_cls=SpaceTimeKey))
Tuple2.add_field('key', MessageField(field_number=1, optional=True, message_cls=First))
Tuple2.add_field('bands', MessageField(field_number=2, optional=True, message_cls=MultibandTile))
