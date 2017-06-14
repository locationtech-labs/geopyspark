"""Contains the various encoding/decoding methods to bring values to/from Python from Scala."""
import array
from functools import partial
import numpy as np
from geopyspark.geopyspark_utils import check_environment
check_environment()

from geopyspark.geotrellis import (Extent, ProjectedExtent, TemporalProjectedExtent, SpatialKey,
                                   SpaceTimeKey)

from geopyspark.protobuf.tileMessages_pb2 import ProtoTile, ProtoMultibandTile, ProtoCellType
from geopyspark.protobuf.extentMessages_pb2 import (ProtoExtent, ProtoProjectedExtent,
                                                ProtoTemporalProjectedExtent)
from geopyspark.protobuf import keyMessages_pb2
from geopyspark.protobuf import tupleMessages_pb2


class ProtoBufRegistry(object):
    """Holds the encoding/decoding methods needed to bring a scala RDD to/from Python."""

    _mapped_data_types = {
        0: 'BIT',
        1: 'BYTE',
        2: 'UBYTE',
        3: 'SHORT',
        4: 'USHORT',
        5: 'INT',
        6: 'FLOAT',
        7: 'DOUBLE'
    }

    # DECODERS

    @classmethod
    def _tile_decoder(cls, tile, data_type=None):
        if not data_type:
            data_type = cls._mapped_data_types[tile.cellType.dataType]

        if data_type == 'BIT':
            arr = np.int8([x for x in tile.sint32Cells]).reshape(tile.rows, tile.cols)
        elif data_type == 'BYTE':
            arr = np.int8([x for x in tile.sint32Cells]).reshape(tile.rows, tile.cols)
        elif data_type == 'UBYTE':
            arr = np.uint8([x for x in tile.uint32Cells]).reshape(tile.rows, tile.cols)
        elif data_type == 'SHORT':
            arr = np.int16([x for x in tile.sint32Cells]).reshape(tile.rows, tile.cols)
        elif data_type == 'USHORT':
            arr = np.uint16([x for x in tile.uint32Cells]).reshape(tile.rows, tile.cols)
        elif data_type == 'INT':
            arr = np.int32([x for x in tile.sint32Cells]).reshape(tile.rows, tile.cols)
        elif data_type == 'FLOAT':
            arr = np.float32([x for x in tile.floatCells]).reshape(tile.rows, tile.cols)
        else:
            arr = np.double([x for x in tile.doubleCells]).reshape(tile.rows, tile.cols)

        return arr

    @classmethod
    def tile_decoder(cls, proto_bytes):
        """Decodes a ``TILE`` into Python.

        Args:
            proto_bytes (bytes): The ProtoBuf encoded bytes of the ProtoBuf class.

        Returns:
            :ref:`Tile <raster>`
        """

        tile = ProtoTile.FromString(proto_bytes)
        data_type = cls._mapped_data_types[tile.cellType.dataType]
        arr = np.array([cls._tile_decoder(tile, data_type)])

        if tile.cellType.hasNoData:
            return {'data': arr, 'no_data_value': tile.cellType.nd, 'data_type': data_type}
        else:
            return {'data': arr, 'data_type': data_type}

    @classmethod
    def _multibandtile_decoder(cls, multibandtile):
        data_type = cls._mapped_data_types[multibandtile.tiles[0].cellType.dataType]
        tiles = np.array([cls._tile_decoder(tile, data_type) for tile in multibandtile.tiles])

        if multibandtile.tiles[0].cellType.hasNoData:
            return {'data': tiles, 'no_data_value': multibandtile.tiles[0].cellType.nd,
                    'data_type': data_type}
        else:
            return {'data': tiles, 'data_type': data_type}

    @classmethod
    def multibandtile_decoder(cls, proto_bytes):
        """Decodes a ``TILE`` into Python.

        Args:
            proto_bytes (bytes): The ProtoBuf encoded bytes of the ProtoBuf class.

        Returns:
            :ref:`Tile <raster>`
        """

        return cls._multibandtile_decoder(ProtoMultibandTile.FromString(proto_bytes))

    @staticmethod
    def extent_decoder(proto_bytes):
        """Decodes an ``Extent`` into Python.

        Args:
            proto_bytes (bytes): The ProtoBuf encoded bytes of the ProtoBuf class.

        Returns:
            :class:`~geopyspark.geotrellis.Extent`
        """

        return Extent.from_protobuf_extent(
            ProtoExtent.FromString(proto_bytes))

    @staticmethod
    def projected_extent_decoder(proto_bytes):
        """Decodes a ``TemporalProjectedExtent`` into Python.

        Args:
            proto_bytes (bytes): The ProtoBuf encoded bytes of the ProtoBuf class.

        Returns:
            :class:`~geopyspark.geotrellis.ProjectedExtent`
        """

        return ProjectedExtent.from_protobuf_projected_extent(
            ProtoProjectedExtent.FromString(proto_bytes))

    @staticmethod
    def temporal_projected_extent_decoder(proto_bytes):
        """Decodes a ``TemproalProjectedExtent`` into Python.

        Args:
            proto_bytes (bytes): The ProtoBuf encoded bytes of the ProtoBuf class.

        Returns:
            :class:`~geopyspark.geotrellis.TemporalProjectedExtent`
        """

        return TemporalProjectedExtent.from_protobuf_temporal_projected_extent(
            ProtoTemporalProjectedExtent.FromString(proto_bytes))

    @staticmethod
    def spatial_key_decoder(proto_bytes):
        """Decodes a ``SpatialKey`` into Python.

        Args:
            proto_bytes (bytes): The ProtoBuf encoded bytes of the ProtoBuf class.

        Returns:
            :class:`~geopyspark.geotrellis.SpatialKey`
        """

        return SpatialKey.from_protobuf_spatial_key(
            keyMessages_pb2.ProtoSpatialKey.FromString(proto_bytes))

    @staticmethod
    def space_time_key_decoder(proto_bytes):
        """Decodes a ``SpaceTimeKey`` into Python.

        Args:
            proto_bytes (bytes): The ProtoBuf encoded bytes of the ProtoBuf class.

        Returns:
            :class:`~geopyspark.geotrellis.SpaceTimeKey`
        """

        return SpaceTimeKey.from_protobuf_space_time_key(
            keyMessages_pb2.ProtoSpaceTimeKey.FromString(proto_bytes))

    @classmethod
    def tuple_decoder(cls, proto_bytes, key_decoder):
        """Decodes a tuple into Python.

        Note:
            The value of the tuple is always assumed to be a :ref:`Tile <raster>`,
            thus, only the decoding method of the key is required.

        Args:
            proto_bytes (bytes): The ProtoBuf encoded bytes of the ProtoBuf class.
            key_decoder (str): The name of the key type of the tuple.

        Returns:
            tuple
        """

        tup = tupleMessages_pb2.ProtoTuple.FromString(proto_bytes)
        multiband = cls._multibandtile_decoder(tup.tiles)

        if key_decoder == "ProjectedExtent":
            return (ProjectedExtent.from_protobuf_projected_extent(tup.projectedExtent),
                    multiband)
        elif key_decoder == "TemporalProjectedExtent":
            return (TemporalProjectedExtent.from_protobuf_temporal_projected_extent(
                tup.temporalProjectedExtent), multiband)
        elif key_decoder == "SpatialKey":
            return (SpatialKey.from_protobuf_spatial_key(tup.spatialKey), multiband)
        else:
            return (SpaceTimeKey.from_protobuf_space_time_key(tup.spaceTimeKey), multiband)

    @classmethod
    def create_partial_tuple_decoder(cls, key_type):
        """Creates a partial, tuple decoder function.

        Args:
            value_type (str): The type of the value in the tuple.

        Returns:
            A partial :meth:`~geopyspark.protobufregistry.ProtoBufRegistry.tuple_decoder`
            function that requires ``proto_bytes`` to execute.
        """

        return partial(cls.tuple_decoder, key_decoder=key_type)

    @classmethod
    def _get_decoder(cls, name):
        if name == "Tile":
            return cls.tile_decoder
        elif name == "MultibandTile":
            return cls.multibandtile_decoder
        elif name == 'ProjectedExtent':
            return cls.projected_extent_decoder
        elif name == 'TemporalProjectedExtent':
            return cls.temporal_projected_extent_decoder
        elif name == "SpatialKey":
            return cls.spatial_key_decoder
        elif name == "SpaceTimeKey":
            return cls.space_time_key_decoder
        else:
            raise Exception("Could not find value type that matches", name)


    # ENCODERS

    @staticmethod
    def _tile_encoder(obj):
        arr = obj['data']
        data_type = obj['data_type']

        (rows, cols) = arr.shape

        tile = ProtoTile()
        cell_type = tile.cellType

        tile.cols = cols
        tile.rows = rows

        if obj.get('no_data_value'):
            cell_type.hasNoData = True
            cell_type.nd = obj['no_data_value']
        else:
            cell_type.hasNoData = False

        if data_type == "BIT":
            cell_type.dataType = ProtoCellType.BIT
            tile.uint32Cells.extend(arr.flatten().tolist())
        elif data_type == "BYTE":
            cell_type.dataType = ProtoCellType.BYTE
            tile.sint32Cells.extend(arr.flatten().tolist())
        elif data_type == "UBYTE":
            cell_type.dataType = ProtoCellType.UBYTE
            tile.uint32Cells.extend(arr.flatten().tolist())
        elif data_type == "SHORT":
            cell_type.dataType = ProtoCellType.SHORT
            tile.sint32Cells.extend(arr.flatten().tolist())
        elif data_type == "USHORT":
            cell_type.dataType = ProtoCellType.USHORT
            tile.uint32Cells.extend(arr.flatten().tolist())
        elif data_type == "INT":
            cell_type.dataType = ProtoCellType.INT
            tile.sint32Cells.extend(arr.flatten().tolist())
        elif data_type == "FLOAT":
            ctype = tile.cellType
            ctype.dataType = ProtoCellType.FLOAT
            tile.floatCells.extend(arr.flatten().tolist())
        else:
            cell_type.dataType = ProtoCellType.DOUBLE
            tile.doubleCells.extend(arr.flatten().tolist())

        return tile

    @classmethod
    def tile_encoder(cls, obj):
        """Decodes a ``TILE`` into bytes.

        Args:
            obj (:ref:`Tile <raster>`): An instance of ``Extent``.

        Returns:
            bytes
        """

        return cls._tile_encoder(obj).SerializeToString()

    @classmethod
    def _multibandtile_encoder(cls, obj):
        if obj['data'].ndim == 2:
            obj['data'] = np.expand_dims(obj['data'], 0)

        band_count = obj['data'].shape[0]
        print(band_count)

        def create_dict(index):
            return {'data': obj['data'][index, :, :], 'no_data_value': obj['no_data_value'],
                    'data_type': obj['data_type']}

        multibandtile = ProtoMultibandTile()
        multibandtile.tiles.extend([cls._tile_encoder(create_dict(x)) for x in range(band_count)])

        return multibandtile

    @classmethod
    def multibandtile_encoder(cls, obj):
        """Decodes a ``TILE`` into bytes.

        Args:
            obj (:ref:`Tile <raster>`): An instance of ``Extent``.

        Returns:
            bytes
        """

        return cls._multibandtile_encoder(obj).SerializeToString()

    @staticmethod
    def extent_encoder(obj):
        """Encodes an ``Extent`` into bytes.

        Args:
            obj (:class:`~geopyspark.geotrellis.Extent`): An instance of ``Extent``.

        Returns:
            bytes
        """

        return obj.to_protobuf_extent.SerializeToString()

    @staticmethod
    def projected_extent_encoder(obj):
        """Encodes a ``ProjectedExtent`` into bytes.

        Args:
            obj (:class:`~geopyspark.geotrellis.ProjectedExtent`): An instance of
                ``ProjectedExtent``.

        Returns:
            bytes
        """

        return obj.to_protobuf_projected_extent.SerializeToString()

    @staticmethod
    def temporal_projected_extent_encoder(obj):
        """Encodes a ``TemproalProjectedExtent`` into bytes.

        Args:
            obj (:class:`~geopyspark.geotrellis.TemporalProjectedExtent`): An instance of
                ``TemporalProjectedExtent``.

        Returns:
            bytes
        """

        return obj.to_protobuf_temporal_projected_extent.SerializeToString()

    @staticmethod
    def spatial_key_encoder(obj):
        """Encodes a ``SpatialKey`` into bytes.

        Args:
            obj (:class:`~geopyspark.geotrellis.SpatialKey`): An instance of ``SpatialKey``.

        Returns:
            bytes
        """

        return obj.to_protobuf_spatial_key.SerializeToString()

    @staticmethod
    def space_time_key_encoder(obj):
        """Encodes a ``SpaceTimeKey`` into bytes.

        Args:
            obj (:class:`~geopyspark.geotrellis.SpaceTimeKey`): An instance of ``SpaceTimeKey``.

        Returns:
            bytes
        """

        return obj.to_protobuf_space_time_key.SerializeToString()

    @classmethod
    def tuple_encoder(cls, obj, key_encoder):
        """Encodes a tuple into bytes.

        Note:
            The value of the tuple is always assumed to be a :ref:`Tile <raster>`,
            thus, only the encoding method of the key is required.

        Args:
            obj (tuple): The tuple to encode.
            key_encoder (str): The name of the key type of the tuple.

        Returns:
           bytes
        """

        tup = tupleMessages_pb2.ProtoTuple()
        tup.tiles.CopyFrom(cls._multibandtile_encoder(obj[1]))

        if key_encoder == "ProjectedExtent":
            tup.projectedExtent.CopyFrom(obj[0].to_protobuf_projected_extent)
        elif key_encoder == "TemporalProjectedExtent":
            tup.temporalProjectedExtent = obj[0].to_protobuf_temporal_projected_exent
        elif key_encoder == "SpatialKey":
            tup.spatialKey.CopyFrom(obj[0].to_protobuf_spatial_key)
        else:
            tup.spaceTimeKey = obj[0].to_protobuf_space_time_key

        return tup.SerializeToString()

    @classmethod
    def create_partial_tuple_encoder(cls, key_type):
        """Creates a partial, tuple encoder function.

        Args:
            key_type (str): The type of the key in the tuple.

        Returns:
            A partial :meth:`~geopyspark.protobufregistry.tuple_encoder` function that requires an
            obj to execute.
        """

        return partial(cls.tuple_encoder, key_encoder=key_type)

    @classmethod
    def _get_encoder(cls, name):
        if name == "Tile":
            return cls.tile_encoder
        elif name == "MultibandTile":
            return cls.multibandtile_encoder
        elif name == 'ProjectedExtent':
            return cls.projected_extent_encoder
        elif name == 'TemporalProjectedExtent':
            return cls.temporal_projected_extent_encoder
        elif name == "SpatialKey":
            return cls.spatial_key_encoder
        elif name == "SpaceTimeKey":
            return cls.space_time_key_encoder
        else:
            raise Exception("Could not find value type that matches", name)
