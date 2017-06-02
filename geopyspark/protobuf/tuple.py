from tile-messages import UByteData, IntData, FloatData, ShortData, ByteData, UShortData, DoubleData, BitData
from protobuf3.fields import MessageField
from key-messages import SpaceTimeKey, SpatialKey
from extent-messages import TemporalProjectedExtent, ProjectedExtent
from protobuf3.message import Message


class First(Message):
    pass


class Second(Message):
    pass


class Tuple2(Message):
    pass

First.add_field('projected_extnet', MessageField(field_number=1, optional=True, message_cls=ProjectedExtent))
First.add_field('temporal_projected_extnet', MessageField(field_number=2, optional=True, message_cls=TemporalProjectedExtent))
First.add_field('spatial_key', MessageField(field_number=3, optional=True, message_cls=SpatialKey))
First.add_field('space_time_key', MessageField(field_number=4, optional=True, message_cls=SpaceTimeKey))
Second.add_field('bit_raster', MessageField(field_number=1, optional=True, message_cls=BitData))
Second.add_field('byte_raster', MessageField(field_number=2, optional=True, message_cls=ByteData))
Second.add_field('ubyte_raster', MessageField(field_number=3, optional=True, message_cls=UByteData))
Second.add_field('short_raster', MessageField(field_number=4, optional=True, message_cls=ShortData))
Second.add_field('ushort_raster', MessageField(field_number=5, optional=True, message_cls=UShortData))
Second.add_field('int_raster', MessageField(field_number=6, optional=True, message_cls=IntData))
Second.add_field('float_raster', MessageField(field_number=7, optional=True, message_cls=FloatData))
Second.add_field('double_raster', MessageField(field_number=8, optional=True, message_cls=DoubleData))
Tuple2.add_field('key', MessageField(field_number=1, optional=True, message_cls=First))
Tuple2.add_field('value', MessageField(field_number=2, optional=True, message_cls=Second))
