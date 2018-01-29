name := "geotools-backend"

resolvers ++= Seq(
  "osgeo" at "http://download.osgeo.org/webdav/geotools",
  "geosolutions" at "http://maven.geo-solutions.it/",
  "Geotoolkit Repo" at "http://maven.geotoolkit.org",
  "Location Tech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots",
  "Location Tech GeoTrellis resleases" at "https://repo.locationtech.org/content/groups/releases",
  Resolver.mavenLocal
)

libraryDependencies ++= Seq(
  "org.apache.spark"            %% "spark-core"            % "2.2.0" % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-s3"         % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-s3-testkit" % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-geotools"   % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-shapefile"  % Version.geotrellis,
  "de.javakaffee" % "kryo-serializers" % "0.38" exclude("com.esotericsoftware", "kryo"),
  "javax.media" % "jai_core" % "1.1.3" % Test from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
)

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case x if x.startsWith("META-INF/services") => MergeStrategy.concat
  case _ => MergeStrategy.first
}

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)
