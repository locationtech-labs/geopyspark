import os
from pathlib import PosixPath

def geopyspark_config(conf, jars, modules):
    jar_uris = map(lambda f: PosixPath(f).as_uri(), jars)
    os.environ['SPARK_CLASSPATH'] = ','.join(jar_uris)
    if conf.get('spark.master') == 'yarn':
        conf.set('spark.yarn.dist.jars', ','.join(jar_uris))
        # TODO: Add modules to spark.yarn.dist.files

    return conf
