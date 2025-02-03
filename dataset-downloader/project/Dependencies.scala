import sbt._

object Dependencies {

  // lijst is alfabetisch gesorteerd
  val catsVersion                = "2.12.0"
  val catsEffectVersion          = "3.5.4"
  val catsParseVersion           = "1.0.0"
  val catsRetryVersion           = "3.1.3"
  val circeMagnoliaVersion       = "0.7.0"
  val circeVersion               = "0.14.9"
  val circeGenericExtrasVersion  = "0.14.3"
  val commonsLangVersion         = "3.16.0"
  val commonsIOVersion           = "2.16.1"
  val disciplineScalaTestVersion = "2.3.0"
  val doobieVersion              = "1.0.0-RC5"
  val fs2Version                 = "3.10.2"
  val fs2DataVersion             = "1.11.1"
  val http4sVersion              = "0.23.15"
  val http4sEmberClientVersion   = "0.23.27"
  val http4sCirceVersion         = "0.23.27"
  val jacksonVersion             = "2.17.2"
  val jenaVersion                = "3.17.0"
  val jsoupVersion               = "1.18.1"
  val junitSBTVersion            = "0.13.3"
  val junitVersion               = "4.13.2"
  val jwarcVersion               = "0.31.1"
  val log4catsVersion            = "2.7.0"
  val logbackLogstashVersion     = "7.3"
  val magnoliaVersion            = "0.17.0"
  val mockSupportVersion         = "2.0.12"
  val monocleVersion             = "2.1.0"
  val parserCombinatorsVersion   = "2.4.0"
  val plantUmlVersion            = "1.2024.3"
  val poiVersion                 = "5.3.0"
  val prometheusVersion          = "0.16.0"
  val refinedVersion             = "0.11.2"
  val scalaCheckVersion          = "1.18.0"
  val scalametaVersion           = "4.9.5"
  val scalatestPlusVersion       = "3.2.18.0"
  val scalatestVersion           = "3.2.19"
  val scalatestEffectsVersion    = "1.5.0"
  val scalacheckEffectsVersion   = "1.0.4"
  val shapelessVersion           = "2.3.12"
  val sqliteDriverVersion        = "3.46.0.1"
  val tikaVersion                = "3.1.0"
  val ujsonVersion               = "3.3.1"

  val cats               = "org.typelevel"             %% "cats-core"                     % catsVersion
  val catsLaws           = "org.typelevel"             %% "cats-laws"                     % catsVersion       % Test
  val catsEffectLaws     = "org.typelevel"             %% "cats-effect-laws"              % catsEffectVersion % Test
  val catsEffect         = "org.typelevel"             %% "cats-effect"                   % catsEffectVersion
  val catsRetry          = "com.github.cb372"          %% "cats-retry"                    % catsRetryVersion
  val fs2                = "co.fs2"                    %% "fs2-core"                      % fs2Version
  val fs2Io              = "co.fs2"                    %% "fs2-io"                        % fs2Version
  val fs2reactiveStreams = "co.fs2"                    %% "fs2-reactive-streams"          % fs2Version
  val fs2Data            = "org.gnieh"                 %% "fs2-data-json"                 % fs2DataVersion
  val commonsLang        = "org.apache.commons"         % "commons-lang3"                 % commonsLangVersion
  val commonsIO          = "commons-io"                 % "commons-io"                    % commonsIOVersion
  val jacksonCore        = "com.fasterxml.jackson.core" % "jackson-core"                  % jacksonVersion
  val jsoup              = "org.jsoup"                  % "jsoup"                         % jsoupVersion
  val jwarc              = "org.netpreserve"            % "jwarc"                         % jwarcVersion
  val tikaCore           = "org.apache.tika"            % "tika-core"                     % tikaVersion
  val tikaParsers        = "org.apache.tika"            % "tika-parsers-standard-package" % tikaVersion

  // sqlite dependencies
  val sqliteDriver = "org.xerial"               % "sqlite-jdbc"      % sqliteDriverVersion
  val doobieCore   = "org.tpolecat"            %% "doobie-core"      % doobieVersion excludeAll (ExclusionRule().withOrganization("com.chuusai"))
  val doobieHikari = "org.tpolecat"            %% "doobie-hikari"    % doobieVersion
  val jena         = "org.apache.jena"          % "apache-jena-libs" % jenaVersion pomOnly ()
  val ujson        = "com.lihaoyi"             %% "ujson"            % ujsonVersion
  val upickle      = "com.lihaoyi"             %% "upickle"          % ujsonVersion
  val plantUml     = "net.sourceforge.plantuml" % "plantuml"         % plantUmlVersion
  val scalameta    = "org.scalameta"           %% "scalameta"        % scalametaVersion

  val scalaTest                  = "org.scalatest"     %% "scalatest"                     % scalatestVersion         % Test
  val scalatestEffects           = "org.typelevel"     %% "cats-effect-testing-scalatest" % scalatestEffectsVersion  % Test
  val scalatestScalaCheckEffects = "org.typelevel"     %% "scalacheck-effect"             % scalacheckEffectsVersion % Test
  val scalaTestPlus              = "org.scalatestplus" %% "scalacheck-1-17"               % scalatestPlusVersion     % Test
  val scalaCheck                 = "org.scalacheck"    %% "scalacheck"                    % scalaCheckVersion        % Test
  val junit                      = "junit"              % "junit"                         % junitVersion             % Test
  val junitSBT                   = "com.github.sbt"     % "junit-interface"               % junitSBTVersion          % Test // to run junit test in SBT build
  val parserCombinators = "org.scala-lang.modules" %% "scala-parser-combinators" % parserCombinatorsVersion % Test
  val catsParse         = "org.typelevel"          %% "cats-parse"               % catsParseVersion
  val magnolia          = "com.propensive"         %% "magnolia"                 % magnoliaVersion          % Test
  val refined           = "eu.timepit"             %% "refined"                  % refinedVersion excludeAll (ExclusionRule().withOrganization("com.chuusai"))
  val refinedScalacheck = "eu.timepit"  %% "refined-scalacheck" % refinedVersion % Test excludeAll (ExclusionRule().withOrganization("org.scalacheck"))
  val shapeless         = "com.chuusai" %% "shapeless"          % shapelessVersion
  val poi = "org.apache.poi" % "poi" % poiVersion excludeAll (
    ExclusionRule().withOrganization("org.slf4j"),
    ExclusionRule().withOrganization("org.apache.log4j"),
    ExclusionRule().withOrganization("ch.qos.logback")
  )
  val poiOoXml = "org.apache.poi" % "poi-ooxml" % poiVersion excludeAll (
    ExclusionRule().withOrganization("org.slf4j"),
    ExclusionRule().withOrganization("org.apache.log4j"),
    ExclusionRule().withOrganization("ch.qos.logback")
  )

  val circeCore          = "io.circe" %% "circe-core"                % circeVersion
  val circeParser        = "io.circe" %% "circe-parser"              % circeVersion
  val circeMagnolia      = "io.circe" %% "circe-magnolia-derivation" % circeMagnoliaVersion excludeAll (ExclusionRule().withOrganization("io.circe"))
  val circeGeneric       = "io.circe" %% "circe-generic"             % circeVersion excludeAll (ExclusionRule().withOrganization("com.chuusai"))
  val circeGenericExtras = "io.circe" %% "circe-generic-extras"      % circeGenericExtrasVersion excludeAll (ExclusionRule().withOrganization("com.chuusai"))
  val circeRefined       = "io.circe" %% "circe-refined"             % circeVersion

  val monocle = Seq(
    "com.github.julien-truffaut" %% "monocle-core"  % monocleVersion,
    "com.github.julien-truffaut" %% "monocle-macro" % monocleVersion
  )

  val disciplineScalaTest = "org.typelevel" %% "discipline-scalatest" % disciplineScalaTestVersion % Test exclude ("org.scalacheck", "scalacheck_2.13")

  private val mockTestExclusions = Seq(
    ExclusionRule().withOrganization("org.slf4j:slf4j").withArtifact("simple"),
    ExclusionRule().withOrganization("be.wegenenverkeer")
  )
  val mockSupport = "be.wegenenverkeer" %% "mock-support-ce3" % mockSupportVersion % Test excludeAll (mockTestExclusions: _*)

  val http4sEmberClient = "org.http4s"          %% "http4s-ember-client"      % http4sEmberClientVersion
  val http4sCirce       = "org.http4s"          %% "http4s-circe"             % http4sCirceVersion
  val log4cats          = "org.typelevel"       %% "log4cats-slf4j"           % log4catsVersion
  val logbackLogstash   = "net.logstash.logback" % "logstash-logback-encoder" % logbackLogstashVersion

  val prometheusSimpleClient = "io.prometheus" % "simpleclient" % prometheusVersion

  val mainTestDependencies = Seq(
    scalaTest,
    scalatestEffects,
    scalatestScalaCheckEffects,
    scalaTestPlus,
    scalaCheck,
    junit,
    junitSBT,
    magnolia,
    refined,
    refinedScalacheck,
    disciplineScalaTest,
    catsLaws,
    catsEffectLaws
  )
  // magnolia en shapeless override is nodig omdat anders java de verkeerde versie meetrekt.
  val overriddenDependencies = Seq(magnolia, shapeless)

}
