name := "Simple Project"
version := "1.0"
scalaVersion := "2.11.7"
libraryDependencies ++= Seq( 
	"org.apache.spark" %% "spark-core" % "1.5.2",
	"org.apache.hadoop" % "hadoop-client" % "2.6.0"
)