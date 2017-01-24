class ProjectedExtent(object):
    def __init__(self, extent, epsg_code):
        self.extent = extent
        self.epsg_code = epsg_code

    def __repr__(self):
        return 'ProjectedExtent(Extent: {}, EPSG: {})'.format(
                self.extent,
                self.epsg_code)
