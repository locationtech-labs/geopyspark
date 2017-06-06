from enum import Enum
from protobuf3.message import Message
from protobuf3.fields import Int32Field, SInt32Field, DoubleField, EnumField, BoolField, UInt32Field, MessageField, FloatField


class ProtoCellType(Message):

    class DataType(Enum):
        BIT = 0
        BYTE = 1
        UBYTE = 2
        SHORT = 3
        USHORT = 4
        INT = 5
        FLOAT = 6
        DOUBLE = 7


class ProtoTile(Message):
    pass


class ProtoMultibandTile(Message):
    pass

ProtoCellType.add_field('dataType', EnumField(field_number=1, optional=True, enum_cls=ProtoCellType.DataType))
ProtoCellType.add_field('nd', DoubleField(field_number=2, optional=True))
ProtoCellType.add_field('hasNoData', BoolField(field_number=3, optional=True))
ProtoTile.add_field('cols', Int32Field(field_number=1, optional=True))
ProtoTile.add_field('rows', Int32Field(field_number=2, optional=True))
ProtoTile.add_field('cellType', MessageField(field_number=3, optional=True, message_cls=ProtoCellType))
ProtoTile.add_field('sint32Cells', SInt32Field(field_number=4, repeated=True))
ProtoTile.add_field('uint32Cells', UInt32Field(field_number=5, repeated=True))
ProtoTile.add_field('floatCells', FloatField(field_number=6, repeated=True))
ProtoTile.add_field('doubleCells', DoubleField(field_number=7, repeated=True))
ProtoMultibandTile.add_field('tiles', MessageField(field_number=1, repeated=True, message_cls=ProtoTile))
