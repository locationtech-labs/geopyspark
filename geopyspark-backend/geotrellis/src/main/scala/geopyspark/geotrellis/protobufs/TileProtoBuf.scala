package geopyspark.geotrellis.protobufs

import geopyspark.geotrellis.ProtoBufCodec
import protos.tileMessages._
import geotrellis.raster._


trait TileProtoBuf {
  implicit def tileProtoBufCodec = new ProtoBufCodec[Tile, ProtoTile] {
    private def cellTypeToMessage(ct: CellType): ProtoCellType = {
      ct match {
        case BitCellType =>
          ProtoCellType(ProtoCellType.DataType.BIT, Byte.MinValue, false)

        case ByteConstantNoDataCellType =>
          ProtoCellType(ProtoCellType.DataType.BYTE, Byte.MinValue, true)
        case ByteCellType =>
          ProtoCellType(ProtoCellType.DataType.BYTE, hasNoData = false)
        case ByteUserDefinedNoDataCellType(v) =>
          ProtoCellType(ProtoCellType.DataType.BYTE, v, true)

        case UByteConstantNoDataCellType =>
          ProtoCellType(ProtoCellType.DataType.UBYTE, 0, true)
        case UByteCellType =>
          ProtoCellType(ProtoCellType.DataType.UBYTE, hasNoData = false)
        case UByteUserDefinedNoDataCellType(v) =>
          ProtoCellType(ProtoCellType.DataType.UBYTE, v, true)

        case ShortConstantNoDataCellType =>
          ProtoCellType(ProtoCellType.DataType.SHORT, Short.MinValue, true)
        case ShortCellType =>
          ProtoCellType(ProtoCellType.DataType.SHORT, hasNoData = false)
        case ShortUserDefinedNoDataCellType(v) =>
          ProtoCellType(ProtoCellType.DataType.SHORT, v, true)

        case UShortConstantNoDataCellType =>
          ProtoCellType(ProtoCellType.DataType.USHORT, 0, true)
        case UShortCellType =>
          ProtoCellType(ProtoCellType.DataType.USHORT, hasNoData = false)
        case UShortUserDefinedNoDataCellType(v) =>
          ProtoCellType(ProtoCellType.DataType.USHORT, v, true)

        case IntConstantNoDataCellType =>
          ProtoCellType(ProtoCellType.DataType.INT, Int.MinValue, true)
        case IntCellType =>
          ProtoCellType(ProtoCellType.DataType.INT, hasNoData = false)
        case IntUserDefinedNoDataCellType(v) =>
          ProtoCellType(ProtoCellType.DataType.INT, v, true)

        case FloatConstantNoDataCellType =>
          ProtoCellType(ProtoCellType.DataType.FLOAT, Float.NaN, true)
        case FloatCellType =>
          ProtoCellType(ProtoCellType.DataType.FLOAT, hasNoData = false)
        case FloatUserDefinedNoDataCellType(v) =>
          ProtoCellType(ProtoCellType.DataType.FLOAT, v, true)

        case DoubleConstantNoDataCellType =>
          ProtoCellType(ProtoCellType.DataType.DOUBLE, Double.NaN, true)
        case DoubleCellType =>
          ProtoCellType(ProtoCellType.DataType.DOUBLE, hasNoData = false)
        case DoubleUserDefinedNoDataCellType(v) =>
          ProtoCellType(ProtoCellType.DataType.DOUBLE, v, true)
      }
    }

    private def messageToCellType(ctm: ProtoCellType): CellType = {
      ctm match {
        case ProtoCellType(ProtoCellType.DataType.BIT, nd, false) =>
          BitCellType

        case ProtoCellType(ProtoCellType.DataType.BYTE, nd, true) =>
          ByteCells.withNoData(Some(nd.toByte))
        case ProtoCellType(ProtoCellType.DataType.BYTE, nd, false) =>
          ByteCells.withNoData(None)

        case ProtoCellType(ProtoCellType.DataType.UBYTE, nd, true) =>
          UByteCells.withNoData(Some(nd.toByte))
        case ProtoCellType(ProtoCellType.DataType.UBYTE, nd, false) =>
          UByteCells.withNoData(None)

        case ProtoCellType(ProtoCellType.DataType.SHORT, nd, true) =>
          ShortCells.withNoData(Some(nd.toShort))
        case ProtoCellType(ProtoCellType.DataType.SHORT, nd, false) =>
          ShortCells.withNoData(None)

        case ProtoCellType(ProtoCellType.DataType.USHORT, nd, true) =>
          UShortCells.withNoData(Some(nd.toShort))
        case ProtoCellType(ProtoCellType.DataType.USHORT, nd, false) =>
          UShortCells.withNoData(None)

        case ProtoCellType(ProtoCellType.DataType.INT, nd, true) =>
          IntCells.withNoData(Some(nd.toInt))
        case ProtoCellType(ProtoCellType.DataType.INT, nd, false) =>
          IntCells.withNoData(None)

        case ProtoCellType(ProtoCellType.DataType.FLOAT, nd, true) =>
          FloatCells.withNoData(Some(nd.toFloat))
        case ProtoCellType(ProtoCellType.DataType.FLOAT, nd, false) =>
          FloatCells.withNoData(None)

        case ProtoCellType(ProtoCellType.DataType.DOUBLE, nd, true) =>
          DoubleCells.withNoData(Some(nd.toDouble))
        case ProtoCellType(ProtoCellType.DataType.DOUBLE, nd, false) =>
          DoubleCells.withNoData(None)
      }
    }

    def encode(tile: Tile): ProtoTile = {
      val protoCellType = cellTypeToMessage(tile.cellType)
      val initialProtoTile =
        ProtoTile(
          cols = tile.cols,
          rows = tile.rows,
          cellType = Some(protoCellType))

      protoCellType.dataType.toString match {
        case ("INT" | "BYTE" | "SHORT") =>
          initialProtoTile.withSint32Cells(tile.toArray())
        case ("UBYTE" | "USHORT" | "BIT") =>
          initialProtoTile.withUint32Cells(tile.toArray())
        case "FLOAT" =>
          initialProtoTile.withFloatCells(tile.asInstanceOf[FloatArrayTile].array)
        case "DOUBLE" =>
          initialProtoTile.withDoubleCells(tile.asInstanceOf[DoubleArrayTile].array)
      }
    }

    def decode(message: ProtoTile): Tile = {
      val messageCellType = message.cellType.get
      val ct = messageToCellType(messageCellType)

      message.cellType.get.dataType.toString match {
        case "BIT" =>
            ArrayTile(message.uint32Cells.toArray, message.cols, message.rows).interpretAs(ct)

        case ("BYTE" | "SHORT") =>
          if (messageCellType.hasNoData)
            ArrayTile(message.sint32Cells.toArray, message.cols, message.rows)
              .interpretAs(ct)
              .map { x => if (x == Int.MinValue) messageCellType.nd.toInt else x }
          else
            ArrayTile(message.sint32Cells.toArray, message.cols, message.rows).interpretAs(ct)

        case ("UBYTE" | "USHORT") =>
          if (messageCellType.hasNoData)
            ArrayTile(message.uint32Cells.toArray, message.cols, message.rows)
              .interpretAs(ct)
              .map { x => if (x == Int.MaxValue) messageCellType.nd.toInt else x }
          else
            ArrayTile(message.uint32Cells.toArray, message.cols, message.rows).interpretAs(ct)

        case "INT" =>
            ArrayTile(message.sint32Cells.toArray, message.cols, message.rows).interpretAs(ct)
        case "FLOAT" =>
          ArrayTile(message.floatCells.toArray, message.cols, message.rows).interpretAs(ct)
        case "DOUBLE" =>
          ArrayTile(message.doubleCells.toArray, message.cols, message.rows).interpretAs(ct)
      }
    }
  }
}


object TileProtoBuf extends TileProtoBuf
