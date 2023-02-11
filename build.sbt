name         := "spark-job"
version      := "1.0"
scalaVersion := "2.12.17"

val sparkVersion =
  "3.3.1"
val slf4jVersion =
  "1.7.16"
val poiVersion =
  "5.2.3"
val xmlbeansVersion =
  "5.1.1"
val commonsVersion =
  "4.4"
val poiooxmlVersion =
  "3.14"

// https://mvnrepository.com/artifact/com.crealytics/spark-excel
libraryDependencies += "com.crealytics" %% "spark-excel" % "3.3.1_0.18.5"

// Note the dependencies are provided
libraryDependencies += "org.apache.spark" %% "spark-core"    % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"     % sparkVersion % "provided"
libraryDependencies += "org.slf4j"         % "slf4j-api"     % slf4jVersion
libraryDependencies += "org.slf4j"         % "slf4j-log4j12" % slf4jVersion
libraryDependencies += "org.apache.poi" % "poi" % poiVersion
libraryDependencies += "org.apache.commons" % "commons-collections4" % commonsVersion
// https://mvnrepository.com/artifact/org.apache.poi/poi-ooxml-schemas
libraryDependencies += "org.apache.poi" % "poi-ooxml-schemas" % poiooxmlVersion
libraryDependencies += "org.apache.xmlbeans" % "xmlbeans" % xmlbeansVersion

// Do not include Scala in the assembled JAR
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// META-INF discarding for the FAT JAR
assemblyMergeStrategy in assembly := {
  case PathList(
        "META-INF",
        xs @ _*
      ) =>
    MergeStrategy.discard
  case x =>
    MergeStrategy.first
}

lazy val spark_run =
  taskKey[Unit](
    "Builds the assembly and ships it to the Spark Cluster"
  )
//spark_run := {
// ("spark_submit " + assembly.value) !
//}
