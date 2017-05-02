lazy val root = (project in file(".")).settings(
  organization := "com.tresata",
  name := "spark-wordtophrase",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8",
  crossScalaVersions := Seq("2.10.6", "2.11.8"),
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % "0.12.3" % "compile",
    "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.3" % "test"
  ),
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
  publishArtifact in Test := false,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  credentials += Credentials(Path.userHome / ".m2" / "credentials_sonatype"),
  pomExtra := (
    <url>https://github.com/tresata/spark-wordtophrase</url>
        <licenses>
      <license>
      <name>Apache 2</name>
              <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      <comments>A business-friendly OSS license</comments>
      </license>
      </licenses>
      <scm>
      <url>git@github.com:tresata/spark-wordtophrase</url>
      <connection>scm:git:git@github.com:tresata/spark-wordtophrase.git</connection>
      </scm>
      <developers>
      <developer>
      <id>gstvolvr</id>
      <name>Gustavo Oliver</name>
      <url>https://github.com/gstvolvr</url>
        </developer>
      <developer>
      <id>koertkuipers</id>
      <name>Koert Kuipers</name>
      <url>https://github.com/koertkuipers</url>
        </developer>
      </developers>
  )
)
