import scala.util.Properties
name := "BioPipeline"
version := "1.0"
version := "0.2-SNAPSHOT"
scalaVersion := "2.11.8"

val DEFAULT_SPARK_2_VERSION = "2.4.3"
val DEFAULT_HADOOP_VERSION = "2.6.5"


lazy val sparkVersion = Properties.envOrElse("SPARK_VERSION", DEFAULT_SPARK_2_VERSION)
lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)


libraryDependencies +=  "org.apache.spark" % "spark-core_2.11" % sparkVersion
libraryDependencies +=  "org.apache.spark" % "spark-sql_2.11" % sparkVersion

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}