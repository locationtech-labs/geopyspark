Catalog
========

The ``Catalog`` method allows the user to read, write, and query GeoTrellis
layers in GeoPySpark. Because GeoPySpark is a Python binding of GeoTrellis,
the layers it saves are GeoTrellis layers.

Accessing the Data
-------------------

GeoPySpark supports various backends to read and save data to and from. These
are the current backends supported:

 - **Local Filesystem**
 - **HDFS**
 - **S3**
 - **Cassandra**
 - **HBase**
 - **Accumulo**

Each of these needs to accessed via the ``URI`` for the given system. Here are
example ``URI``\s for each:

 - **Local** Filesystem: file://my_folder/my_catalog/
 - **HDFS**: hdfs://my_folder/my_catalog/
 - **S3**: s3://my_bucket/my_catalog/
 - **Cassandra**: cassandra:name?username=user&password=pass&host=host1&keyspace=key&table=table
 - **HBase**: hbase://zoo1, zoo2: port/table
 - **Accumulo**: accumulo://username:password/zoo1, zoo2/instance/table

It is important to note that neither HBase or Accumulo have native support for
``URI``\s. Thus, GeoPySpark uses its own pattern for these two systems.

The ``URI`` for HBase follows this pattern:
 - hbase://zoo1, zoo2, ..., zooN: port/table

The ``URI`` for Accumulo follows this pattern:
 - accumulo://username:password/zoo1, zoo2/instance/table

Some backends require various options to be set, and each function in
``Catalog`` has an ``options`` parameter where they can be set. These are
backends that need these additional values and the options to set for each.

Fields that can be set for Cassandra:
 - **replicationStrategy** (str, optional): If not specified, then
   'SimpleStrategy' will be used.
 - **replicationFactor** (int, optional): If not specified, then 1 will be used.
 - **localDc** (str, optional): If not specified, then 'datacenter1' will be used.
 - **usedHostsPerRemoteDc** (int, optional): If not specified, then 0 will be used.
 - **allowRemoteDCsForLocalConsistencyLevel** (int, optional): If you'd like this feature,
     then the value would be 1, Otherwise, the value should be 0. If not specified,
     then 0 will be used.

Fields that can be set for HBase:
 - **master** (str, optional): If not specified, then ``null`` will be used.


A Note on the Formatting of Rasters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A small, but important, note needs to be made about how rasters that are saved
and/or read in are formatted in GeoPySpark. All rasters will be treated as a
MultibandTile. Regardless if they were one to begin with. This was a design
choice that was made to simplify both the backend and the API of GeoPySpark.
