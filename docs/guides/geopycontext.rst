GeoPyContext
=============

:class:`~geopyspark.geopycontext.GeoPyContext` is a class that acts as a
wrapper for ``SparkContext`` in GeoPySpark. Why have such a class? It has to do
with how the Python and Scala code communicate with one another. By hooking
into the JVM, the Python side is able to access classes, objects, and functions
from Scala. This also holds true for Scala, which is able to send over values
to Python.

Before being sent to the other side, though, the values must be formatted in
such a way so they can be serialized/deserialized. ``GeoPyContext`` makes this a
little easier by providing methods that will prepare the data before it is
sent over. This is why a ``GeoPyContext`` instance is needed for almost all
functions and class constructors.


Initializing GeoPyContext
--------------------------

Initializing ``GeoPyContext`` can be done through two different methods:
either by giving it an existing ``SparkContext``, or by passing in the
arguments used to construct a ``SparkContext``.

.. code:: python

   from geopyspark.geopycontext import GeoPyContext

   from pyspark import SparkContext

   # Using an existing SparkContext.
   sc = SparkContext(appName="example", master="local[*]")

   geopysc = GeoPyContext(sc)

   # Using SparkContext args.
   geopysc = GeoPyContext(appName="example", master="local[*]")
