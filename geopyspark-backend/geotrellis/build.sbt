name := "geotrellis-backend"

libraryDependencies ++= Seq(
  "com.typesafe.akka"           %% "akka-http"             % "10.0.10",
  "com.typesafe.akka"           %% "akka-http-spray-json"  % "10.0.10",
  "net.sf.py4j"                 %  "py4j"                  % "0.10.6",
  "org.apache.spark"            %% "spark-core"            % "2.3.0" % "provided",
  "org.apache.commons"          % "commons-math3"          % "3.6.1",
  "org.locationtech.geotrellis" %% "geotrellis-s3"         % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-s3-testkit" % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % Version.geotrellis,
  "com.azavea.geotrellis"       %% "geotrellis-contrib-vlm" % Version.geotrellisContrib
)

assemblyShadeRules in assembly := {
  val shadePackage = "org.locationtech.geopyspark.shaded"
  Seq(
    ShadeRule.rename("org.apache.avro.**" -> s"$shadePackage.org.apache.avro.@1")
      .inLibrary("org.locationtech.geotrellis" %% "geotrellis-spark" % Version.geotrellis).inAll,

    ShadeRule.rename("com.typesafe.scalalogging.**" -> s"$shadePackage.com.typesafe.scalalogging.@1")
      .inLibrary("org.locationtech.geotrellis" %% "geotrellis-spark" % Version.geotrellis).inAll,
    ShadeRule.rename("shapeless.**" -> s"$shadePackage.shapeless.@1").inAll
  )
}

Test / fork := true
Test / parallelExecution := false
Test / testOptions += Tests.Argument("-oDF")

javaOptions ++= Seq("-Xms1024m", "-Xmx6144m", "-Djava.library.path=/usr/local/lib")

assemblyMergeStrategy in assembly := {
  case s if s.startsWith("META-INF/services") => MergeStrategy.concat
  case "reference.conf" | "application.conf"  => MergeStrategy.concat
  case "META-INF/ECLIPSE.RSA" | "META-INF/ECLIPSE.SF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case "META-INF/ECLIPSE_.RSA" | "META-INF/ECLIPSE_.SF" => MergeStrategy.discard
  case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)
