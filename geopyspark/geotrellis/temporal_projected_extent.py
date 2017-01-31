class TemporalProjectedExtent(object):
    def __init__(self, extent, epsg_code, instant):
        self.extent = extent
        self.epsg_code = epsg_code
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
        return 'TemporalProjectedExtent(Extemt: {}, EPSG: {}, Instant: {})'.format(
                self.extent,
                self.epsg_code,
                self.instan)
