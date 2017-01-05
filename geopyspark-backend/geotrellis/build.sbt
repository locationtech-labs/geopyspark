name := "geotrellis-backend"

resolvers ++= Seq(
  "Location Tech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots",
  Resolver.mavenLocal
)

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" % "geotrellis-spark_2.11" % "1.0.4-SNAPSHOT",
  "org.locationtech.geotrellis" % "geotrellis-spark-etl_2.11" % "1.0.4-SNAPSHOT",
  "org.apache.spark" %% "spark-core" % "1.2.2" % "provided"
)

assemblyMergeStrategy in assembly := {
  case "referejce.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}
