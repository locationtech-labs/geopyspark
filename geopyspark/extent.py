class Extent(object):
    def __init__(self, xmin, ymin, xmax, ymax):
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax

    def __repr__(self):
        return "Extent({}, {}, {}, {})".format(self.xmin, self.ymin, self.xmax, self.ymax)
