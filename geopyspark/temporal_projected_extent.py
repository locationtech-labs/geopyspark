class TemporalProjectedExtent(object):
    def __init__(self, extent, epsg_code, instant):
        self.extent = extent
        self.epsg_code = epsg_code
        self.instant = instant

    def __repr__(self):
        return 'TemporalProjectedExtent(Extemt: {}, EPSG: {}, Instant: {})'.format(
                self.extent,
                self.epsg_code,
                self.instan)
