class SpatialKey(object):
    def __init__(self, col, row):
        self.col = col
        self.row = row

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return NotImplemented

    def __repr__(self):
        return "SpatialKey(%d, %d)" % (self.col, self.row)


class SpaceTimeKey(object):
    def __init__(self, col, row, instant):
        self.col = col
        self.row = row
        self.instant = instant

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return NotImplemented

    def __repr__(self):
        return "SpaceTimeKey(%d, %d, %d)" % (self.col, self.row, self.instant)
