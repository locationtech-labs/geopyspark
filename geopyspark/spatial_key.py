class SpatialKey(object):
    def __init__(self, col, row):
        self.col = col
        self.row = row

    def __repr__(self):
        return "SpatialKey(%d,%d)" % (self.col, self.row)
