credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

addSbtPlugin("com.typesafe.play"   % "sbt-plugin"    % "2.7.3")
libraryDependencies += "org.vafer" % "jdeb"          % "1.7" artifacts Artifact("jdeb", "jar", "jar")
addSbtPlugin("com.eed3si9n"        % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("io.get-coursier"     % "sbt-coursier"  % "1.0.3")
addSbtPlugin("com.geirsson"        % "sbt-scalafmt"  % "1.5.1")
addSbtPlugin("ch.epfl.scala"       % "sbt-scalafix"  % "0.9.29")
addSbtPlugin("com.github.sbt"      % "sbt-dynver"    % "5.0.1")
addSbtPlugin("com.timushev.sbt"    % "sbt-updates"   % "0.6.3")
