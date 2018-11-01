import scala.util.Properties

object Environment {
  lazy val sha: Option[String] = Properties.envOrNone("GEOPYSPARK_VERSION_SUFFIX")
}
