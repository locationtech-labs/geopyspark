name := "geotrellis-backend"

resolvers ++= Seq(
  "Location Tech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots",
  Resolver.mavenLocal
)

libraryDependencies ++= Seq(
  "org.apache.spark"            %% "spark-core"            % "2.0.0" % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-accumulo"   % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-cassandra"  % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-hbase"      % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-s3"         % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-s3-testkit" % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % Version.geotrellis,
  "org.spire-math" %% "spire" % "0.13.0"
)

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}
