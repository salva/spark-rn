name := "spark-rn"

version := "0.0.1"

scalaVersion := "2.12.11"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.0-SNAPSHOT" // % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0-SNAPSHOT" // % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.1.0-SNAPSHOT" // % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.0-SNAPSHOT"

libraryDependencies += "org.scalanlp" %% "breeze" % "1.0"
libraryDependencies += "org.scalanlp" %% "breeze-natives" % "1.0"

organization := "com.github.salva"
organizationHomepage := Some(url("https://github.com/salva"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/salva/spark-rn"),
    "https://github.com/salva/spark-rn.git"
  )
)

developers := List (
  Developer(
    id = "salva",
    name = "Salvador Fandiño",
    email = "sfandino@yahoo.com",
    url = url("https://github.com/salva")
  )
)

description := "Algorithms for vectors of small dimension (ℝ^n)"
licenses := Seq("APL2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://github.com/salva/spark-rn"))

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

/*
assemblyExcludedJars in assembly := {
  val exclusions = Seq("sourcecode_", "scala-library-",  "dbutils-", "cats-")
  (fullClasspath in assembly)
    .value
    .filter(dep => exclusions.exists(ex => dep.data.getName.startsWith(ex)))
}
*/


publishMavenStyle := true