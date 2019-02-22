apt-get update -y && apt-get install -y wget python3-pip awscli && \
pushd geopyspark-backend && ./sbt "project geotrellis-backend" assembly && \
cp geotrellis/target/scala-2.11/geotrellis-backend-assembly-*.jar ../geopyspark/jars && popd && \
if [ ! -f archives/spark-2.3.2-bin-hadoop2.7.tgz ]; then
  cd archives
  wget http://apache.cs.utah.edu/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz
  cd ~-
fi && \
tar -xvf archives/spark-2.3.2-bin-hadoop2.7.tgz && \
pip3 install -r requirements.txt && \
pip3 install pyproj && \
pip3 install matplotlib==2.0.0 && \
pip3 install pylint && \
pip3 install colortools==0.1.2 && \
pip3 install awscli && \
export PATH=$PATH:$HOME/.local/bin && \
pip3 install . && \
export SPARK_HOME=./spark-2.3.2-bin-hadoop2.7/ && \
./scripts/test_script.sh && \
if [[ $TRAVIS_PYTHON_VERSION != "3.3" ]]; then
  pylint geopyspark;
fi
