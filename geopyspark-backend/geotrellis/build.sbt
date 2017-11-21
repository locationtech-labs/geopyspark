name := "geotrellis-backend"

resolvers ++= Seq(
  "Location Tech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots",
  "Location Tech GeoTrellis resleases" at "https://repo.locationtech.org/content/groups/releases",
  Resolver.mavenLocal
)

libraryDependencies ++= Seq(
  "org.apache.spark"            %% "spark-core"            % "2.0.0" % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-accumulo"   % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-cassandra"  % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-hbase"      % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-s3"         % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-s3-testkit" % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % Version.geotrellis,
  "org.typelevel"               %% "cats"                  % "0.9.0",
  "com.typesafe.akka"     %% "akka-actor"                  % "2.5.6",
  "com.typesafe.akka"     %% "akka-http"                   % "10.0.10",
  "com.typesafe.akka"     %% "akka-http-spray-json"        % "10.0.10",
  "net.sf.py4j"           % "py4j"                         % "0.10.5"
)

dependencyOverrides += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.105"

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
