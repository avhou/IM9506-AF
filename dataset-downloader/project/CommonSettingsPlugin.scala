import sbt.Keys._
import sbt._
import coursier.CoursierPlugin.autoImport._
import scala.util.Try

object CommonSettingsPlugin extends AutoPlugin {

  override def trigger = allRequirements
  override def projectSettings =
    Seq(
      organization := "be.avhconsult",
      credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
      coursierUseSbtCredentials := true,
      Test / parallelExecution := sys.props
        .get("ci.parallel.test.execution")
        .orElse(sys.env.get("ci.parallel.test.execution"))
        .flatMap(value => Try(value.toBoolean).toOption)
        .getOrElse(true),
      updateOptions := updateOptions.value.withCachedResolution(true),
      dependencyOverrides ++= Dependencies.overriddenDependencies,
      Compile / packageDoc / publishArtifact := false,
      packageDoc / publishArtifact := false,
      Compile / doc / sources := Seq.empty,
      ThisBuild / shellPrompt := { state =>
        Project.extract(state).currentRef.project + "> "
      }
    )

  def publishSettings: Seq[Setting[_]] =
    Seq(
      ThisBuild / publishTo := {
        val nexus = "https://collab.mow.vlaanderen.be/artifacts/repository/"
        if (isSnapshot.value)
          Some("collab snapshots" at nexus + "maven-snapshots")
        else
          Some("collab releases" at nexus + "maven-releases")
      },
      Compile / publishArtifact := true,
      Test / publishArtifact := true,
      publishMavenStyle := true
    )

}
