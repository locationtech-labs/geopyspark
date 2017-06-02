from protobuf3.fields import StringField, FloatField, SInt32Field, MessageField, DoubleField, Int32Field
from protobuf3.message import Message


class TileInfo(Message):
    pass


class Int32Tile(Message):
    pass


class UInt32Tile(Message):
    pass


class BitTile(Message):
    pass


class ByteTile(Message):
    pass


class UByteTile(Message):
    pass


class ShortTile(Message):
    pass


class UShortTile(Message):
    pass


class IntTile(Message):
    pass


class FloatTile(Message):
    pass


class DoubleTile(Message):
    pass


class MultibandTile(Message):

    class TileType(Message):
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
BitTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=UInt32Tile))
ByteTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=Int32Tile))
UByteTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=UInt32Tile))
ShortTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=Int32Tile))
UShortTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=UInt32Tile))
IntTile.add_field('tile', MessageField(field_number=1, optional=True, message_cls=Int32Tile))
FloatTile.add_field('info', MessageField(field_number=1, optional=True, message_cls=TileInfo))
FloatTile.add_field('cells', FloatField(field_number=2, repeated=True))
FloatTile.add_field('noDataValue', FloatField(field_number=3, optional=True))
DoubleTile.add_field('info', MessageField(field_number=1, optional=True, message_cls=TileInfo))
DoubleTile.add_field('cells', DoubleField(field_number=2, repeated=True))
DoubleTile.add_field('noDataValue', DoubleField(field_number=3, optional=True))
MultibandTile.TileType.add_field('bit_tiles', MessageField(field_number=1, optional=True, message_cls=BitTile))
MultibandTile.TileType.add_field('byte_tiles', MessageField(field_number=2, optional=True, message_cls=ByteTile))
MultibandTile.TileType.add_field('ubyte_tiles', MessageField(field_number=3, optional=True, message_cls=UByteTile))
MultibandTile.TileType.add_field('short_tiles', MessageField(field_number=4, optional=True, message_cls=ShortTile))
MultibandTile.TileType.add_field('ushort_tiles', MessageField(field_number=5, optional=True, message_cls=UShortTile))
MultibandTile.TileType.add_field('int_tiles', MessageField(field_number=6, optional=True, message_cls=IntTile))
MultibandTile.TileType.add_field('float_tiles', MessageField(field_number=7, optional=True, message_cls=FloatTile))
MultibandTile.TileType.add_field('double_tiles', MessageField(field_number=8, optional=True, message_cls=DoubleTile))
MultibandTile.add_field('tiles', MessageField(field_number=1, repeated=True, message_cls=MultibandTile.TileType))
