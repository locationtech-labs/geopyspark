

class Histogram(object):
    def __init__(self, scala_histogram):
        self.scala_histogram = scala_histogram

    @classmethod
    def from_tiled_layer(cls, tiled_layer):
        return cls(tiled_layer.get_histogram())

    def min(self):
        return self.scala_histogram.minValue().get()

    def max(self):
        return self.scala_histogram.maxValue().get()

    def min_max(self):
        tup = self.scala_histogram.minMaxValues().get()

        return (tup._1(), tup._2())

    def mean(self):
        return self.scala_histogram.mean().get()

    def mode(self):
        return self.scala_histogram.mode().get()

    def median(self):
        return self.scala_histogram.mean().get()

    def values(self):
        return list(self.scala_histogram.values())

    def cdf(self):
        cdfs = list(self.scala_histogram.cdf())

        return [(cdf._1(), cdf._2()) for cdf in cdfs]

    def bucket_count(self):
        return self.scala_histogram.bucketCount()

    def quantile_breaks(self):
        return list(self.scala_histogram.quantileBreaks())

    def merge(self, other_histogram):
        return Histogram(self.scala_histogram.merge(other_histogram.scala_histogram))
