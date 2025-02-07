import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import sbtbuildinfo.BuildInfoKeys.buildInfoKeys
import BuildSettings._
import Dependencies._
import sbt.{ Test, _ }

import scala.sys.process.Process

name := "dataset-downloader"
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
Test / testOptions += Tests.Argument("-oDF")
ThisBuild / publishMavenStyle := true
ThisBuild / dynverSonatypeSnapshots := true
ThisBuild / dynverSeparator := "-"
ThisBuild / assumedEvictionErrorLevel := Level.Info
ThisBuild / assumedVersionScheme := VersionScheme.EarlySemVer
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
Compile / run / fork := true

updateOptions := updateOptions.value.withCachedResolution(true)
Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val downloader = {

  val mainDeps = Seq(
    cats,
    catsEffect,
    refined,
    doobieCore,
    doobieHikari,
    sqliteDriver,
    http4sEmberClient,
    http4sCirce,
    fs2,
    jsoup,
    jwarc,
    tikaCore,
    tikaParsers,
    circeCore,
    circeParser,
    circeMagnolia
  )

  subProject("downloader")
    .settings(
      libraryDependencies ++= mainDeps ++ mainTestDependencies
    )
    .enablePlugins(UniversalPlugin)
    .enablePlugins(JavaAppPackaging)
}

//----------------------------------------------------------------
lazy val main = mainProject(
  downloader
)
