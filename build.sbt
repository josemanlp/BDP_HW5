import sbt.Keys.libraryDependencies

lazy val commonSettings=Seq(
  version := "1.0",
  scalaVersion := "2.11.11",
  fork in run := true

)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name:="SparkStudy",

    libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1",
    libraryDependencies +="org.apache.spark" %% "spark-sql" %"2.2.1",
    libraryDependencies +="org.apache.spark" %% "spark-mllib" % "2.2.1",
    libraryDependencies +="org.apache.spark" %% "spark-streaming" % "2.2.0",
    libraryDependencies += "org.apache.bahir" % "spark-streaming-twitter_2.11" % "2.2.0"


  )
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"




