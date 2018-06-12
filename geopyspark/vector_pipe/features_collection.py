from json import loads
from geopyspark import create_python_rdd
from geopyspark.geotrellis.protobufserializer import ProtoBufSerializer
from geopyspark.vector_pipe.vector_pipe_protobufcodecs import feature_decoder, feature_encoder


class FeaturesCollection(object):
    """Represents a collection of features from OSM data. A ``feature`` is
    a geometry that is derived from an OSM Element with that Element's associated metadata.
    These ``feature``\s are grouped together by their representation in OSM.

    There are 3 different types of feature geometries:
        - ``Node``\s
        - ``Way``\s
        - ``Relation``\s

    While ``Nodes``\s will always contain ``Point``\s, ``Way``\s and ``Relation``\s can contain
    any number of different geometries.

    Args:
        scala_features (py4j.JavaObject): The Scala representation of ``FeaturesCollection``.

    Attributes:
        scala_features (py4j.JavaObject): The Scala representation of ``FeaturesCollection``.
    """

    def __init__(self, scala_features):
        self.scala_features = scala_features

    def get_node_features_rdd(self):
        """Returns the geometrical feature of each ``Node`` in
        the ``FeaturesCollection`` as a :class:`~geopyspark.vector_pipe.Feature`
        in a Python RDD.

        Returns:
            ``RDD[Feature]``
        """

        return self._get_rdd(self.scala_features.toProtoNodes())

    def get_way_features_rdd(self):
        """Returns the geometrical feature of each ``Way`` in
        the ``FeaturesCollection`` as a :class:`~geopyspark.vector_pipe.Feature`
        in a Python RDD.

        Returns:
            ``RDD[Feature]``
        """

        return self._get_rdd(self.scala_features.toProtoWays())

    def get_relation_features_rdd(self):
        """Returns the geometrical feature of each ``Relation`` in
        the ``FeaturesCollection`` as a :class:`~geopyspark.vector_pipe.Feature`
        in a Python RDD.

        Returns:
            ``RDD[Feature]``
        """

        return self._get_rdd(self.scala_features.toProtoRelations())

    def _get_rdd(self, jrdd):
        ser = ProtoBufSerializer(feature_decoder, feature_encoder)

        return create_python_rdd(jrdd, ser)

    def get_node_tags(self):
        """Returns all of the unique tags for all of the ``Node``\s in the
        ``FeaturesCollection`` as a ``dict``. Both the keys and values of the
        ``dict`` will be ``str``\s.

        Returns:
            dict
        """

        return loads(self.scala_features.getNodeTags())

    def get_way_tags(self):
        """Returns all of the unique tags for all of the ``Way``\s in the
        ``FeaturesCollection`` as a ``dict``. Both the keys and values of the
        ``dict`` will be ``str``\s.

        Returns:
            dict
        """

        return loads(self.scala_features.getWayTags())

    def get_relation_tags(self):
        """Returns all of the unique tags for all of the ``Relation``\s in the
        ``FeaturesCollection`` as a ``dict``. Both the keys and values of the
        ``dict`` will be ``str``\s.

        Returns:
            dict
        """

        return loads(self.scala_features.getRelationTags())
