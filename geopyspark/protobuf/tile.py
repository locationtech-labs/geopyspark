from protobuf3.fields import SInt32Field, StringField, MessageField, FloatField, Int32Field, DoubleField
from protobuf3.message import Message


class TileInfo(Message):
    pass


class Int32Tile(Message):
    pass


class UInt32Tile(Message):
    pass


class BitData(Message):

    class BitArrayTile(Message):
        pass


class ByteData(Message):

    class ByteArrayTile(Message):
        pass


class UByteData(Message):

    class UByteArrayTile(Message):
        pass


class ShortData(Message):

    class ShortArrayTile(Message):
        pass


class UShortData(Message):

    class UShortArrayTile(Message):
        pass


class IntData(Message):

    class IntArrayTile(Message):
        pass


class FloatData(Message):

    class FloatArrayTile(Message):
        pass


class DoubleData(Message):

    class DoubleArrayTile(Message):
        pass

TileInfo.add_field('name', StringField(field_number=1, optional=True))
TileInfo.add_field('cols', Int32Field(field_number=2, optional=True))
TileInfo.add_field('rows', Int32Field(field_number=3, optional=True))
Int32Tile.add_field('info', MessageField(field_number=1, optional=True, message_cls=TileInfo))
Int32Tile.add_field('cells', SInt32Field(field_number=2, repeated=True))
Int32Tile.add_field('noDataValue', SInt32Field(field_number=3, optional=True))
UInt32Tile.add_field('info', MessageField(field_number=1, optional=True, message_cls=TileInfo))
UInt32Tile.add_field('cells', Int32Field(field_number=2, repeated=True))
UInt32Tile.add_field('noDataValue', Int32Field(field_number=3, optional=True))
BitData.BitArrayTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=UInt32Tile))
BitData.add_field('bands', MessageField(field_number=1, repeated=True, message_cls=BitData.BitArrayTile))
ByteData.ByteArrayTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=Int32Tile))
ByteData.add_field('bands', MessageField(field_number=1, repeated=True, message_cls=ByteData.ByteArrayTile))
UByteData.UByteArrayTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=UInt32Tile))
UByteData.add_field('bands', MessageField(field_number=1, repeated=True, message_cls=UByteData.UByteArrayTile))
ShortData.ShortArrayTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=Int32Tile))
ShortData.add_field('bands', MessageField(field_number=1, repeated=True, message_cls=ShortData.ShortArrayTile))
UShortData.UShortArrayTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=UInt32Tile))
UShortData.add_field('bands', MessageField(field_number=1, repeated=True, message_cls=UShortData.UShortArrayTile))
IntData.IntArrayTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=Int32Tile))
IntData.add_field('bands', MessageField(field_number=1, repeated=True, message_cls=IntData.IntArrayTile))
FloatData.FloatArrayTile.add_field('info', MessageField(field_number=1, optional=True, message_cls=TileInfo))
FloatData.FloatArrayTile.add_field('cells', FloatField(field_number=2, repeated=True))
FloatData.FloatArrayTile.add_field('noDataValue', FloatField(field_number=3, optional=True))
FloatData.add_field('bands', MessageField(field_number=1, repeated=True, message_cls=FloatData.FloatArrayTile))
DoubleData.DoubleArrayTile.add_field('info', MessageField(field_number=1, optional=True, message_cls=TileInfo))
DoubleData.DoubleArrayTile.add_field('cells', DoubleField(field_number=2, repeated=True))
DoubleData.DoubleArrayTile.add_field('noDataValue', DoubleField(field_number=3, optional=True))
DoubleData.add_field('bands', MessageField(field_number=1, repeated=True, message_cls=DoubleData.DoubleArrayTile))
