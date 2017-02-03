class ProjectedExtent(object):
    def __init__(self, extent, epsg_code):
        self.extent = extent
        self.epsg_code = epsg_code

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return NotImplemented

    def __repr__(self):
        return 'ProjectedExtent(Extent: {}, EPSG: {})'.format(
                self.extent,
                self.epsg_code)
