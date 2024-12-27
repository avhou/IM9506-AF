import CommonSettingsPlugin.publishSettings
import com.typesafe.sbt.packager.universal.UniversalPlugin
import sbt.Keys.*
import sbt.*

object BuildSettings {

  lazy val scala213               = "2.13.14"
  lazy val supportedScalaVersions = List(scala213)

  val compilerOptions: Seq[String] = {
    Seq(
      "-deprecation", // warning and location for usages of deprecated APIs
      "-encoding",
      "UTF-8",
      "-explaintypes",
      "-feature", // warning and location for usages of features that should be imported explicitly
      "-language:existentials",
      "-language:experimental.macros",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-language:reflectiveCalls",
      "-unchecked", // additional warnings where generated code depends on assumptions
      "-Xcheckinit",
      "-Xsource:3",
      "-Xfatal-warnings",
      "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
      "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
      "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
      "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
      "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
      "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
      "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
      "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
      "-Xlint:option-implicit", // Option.apply used implicit view.
      "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
      "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
      "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
      "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
      "-Ywarn-dead-code", // Warn when dead code is identified.
      "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
      "-Ywarn-numeric-widen", // Warn when numerics are widened.
      "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
      "-Ywarn-unused:locals", // Warn if a local definition is unused.
      "-Ywarn-unused:params", // Warn if a value parameter is unused.
      "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
      "-Ywarn-macros:after", // Do unused checks after macro expansion
      "-Ywarn-unused:privates", // Warn if a private member is unused.
      "-Ywarn-value-discard", // Warn when non-Unit expression results are unused.
      "-release:11",
      "-Ymacro-annotations" // integration of macro paradise in scala 2.13
    )
  }

  def subProject(moduleName: String): Project = {
    Project(
      id = s"$moduleName",
      base = file("modules/" + moduleName)
    ).settings(
      autoCompilerPlugins := true,
      publishSettings,
      scalaVersion := scala213,
      crossScalaVersions := supportedScalaVersions,
      addCompilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1"),
      addCompilerPlugin("org.typelevel" %% "kind-projector"     % "0.13.3" cross CrossVersion.full),
      addCompilerPlugin("org.scalameta"  % "semanticdb-scalac"  % "4.9.5" cross CrossVersion.full),
      ThisBuild / scalacOptions ++= compilerOptions,
      ThisBuild / Compile / run / fork := true,
      testOptions += Tests.Argument("-oF")
    )
  }

  def mainProject(modules: ProjectReference*): Project = {
    Project(
      id = "dataset-downloader",
      base = file(".")
    ).settings(
      publishArtifact := false,
      crossScalaVersions := supportedScalaVersions,
      testOptions += Tests.Argument("-oF")
    ).aggregate(modules: _*)
  }

}
