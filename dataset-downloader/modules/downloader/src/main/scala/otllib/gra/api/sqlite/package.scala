package otllib.gra.api
package object sqlite {}
//import be.wegenenverkeer.infra.infrastructure.stack.either.EitherThrowable
//import cats.data.NonEmptyChain
//import cats.effect.*
//import cats.syntax.all.*
//import doobie.*
//import doobie.hikari.HikariTransactor
//import doobie.implicits.*
//import eu.timepit.refined.types.all.*
//import otllib.gra.api.model.*
//import otllib.gra.api.validation.GRAValidator
//
//import java.net.URI
//import scala.util.Try
//import scala.util.control.NoStackTrace
//
//package object sqlite {
//
//  final case class GRAReaderFout(fouten: Errors)
//      extends RuntimeException(s"het uitlezen van het GRA uit sqlite is mislukt, fouten : ${fouten.toList.mkString(", ")}")
//      with NoStackTrace
//
//  def transactor[F[_]: Async](name: String): Resource[F, HikariTransactor[F]] =
//    for {
//      ce <- ExecutionContexts.cachedThreadPool[F] // our connect EC
//      xa <- HikariTransactor.newHikariTransactor[F](
//              "org.sqlite.JDBC", // driver classname
//              s"jdbc:sqlite:$name", // connect URL
//              "", // username
//              "", // password
//              ce // await connection here
//            )
//    } yield xa
//
//  object types {
//    type GroeperingStartRow               = (Int, String, Option[String], Option[String])
//    type GroeperingOpbouwRow              = (Int, String, String, Option[String], String, Option[String], String, Option[Boolean])
//    type GroeperingAttribuutOverervingRow = (Int, String, Option[String], Option[String])
//    type GroeperingAgentOverervingRow     = (Int, String, Option[String], Option[String])
//    type GroeperingRelatiesRow            = (String, String, String, String, Option[String])
//    type GroeperingAttribuutRow           = (String, String, String, String, String, Option[String], String, String, Boolean)
//    type GroeperingClassRow               = (String, String, String, Option[String], Boolean, Boolean)
//    type GeneralInfoRow                   = (String, String)
//  }
//
//  object queries {
//    import types.*
//
//    val groeperingStartQuery: ConnectionIO[List[GroeperingStartRow]] =
//      sql"select id, groepering_uri, start_uri, start_conditie from GroeperingStart"
//        .query[GroeperingStartRow]
//        .to[List]
//
//    val groeperingOpbouwQuery: ConnectionIO[List[GroeperingOpbouwRow]] =
//      sql"select groeperingId, bron_uri, relatie_uri, relatie_conditie, doel_uri, doel_conditie, richting, collect_geo from GroeperingOpbouw"
//        .query[GroeperingOpbouwRow]
//        .to[List]
//        .exceptSql(_ =>
//          sql"select groeperingId, bron_uri, relatie_uri, relatie_conditie, doel_uri, doel_conditie, richting, 1 from GroeperingOpbouw"
//            .query[GroeperingOpbouwRow]
//            .to[List]
//        )
//
//    val groeperingAttribuutOverervingQuery: ConnectionIO[List[GroeperingAttribuutOverervingRow]] =
//      sql"select groeperingId, bron_uri, overerving_omschrijving, overerving_level from GroeperingAttribuutOvererving"
//        .query[GroeperingAttribuutOverervingRow]
//        .to[List]
//        .exceptSql(_ =>
//          sql"select groeperingId, bron_uri, overerving_omschrijving, '!1' from GroeperingAttribuutOvererving"
//            .query[GroeperingAttribuutOverervingRow]
//            .to[List]
//        )
//
//    val groeperingAgentOverervingQuery: ConnectionIO[List[GroeperingAgentOverervingRow]] =
//      sql"select groeperingId, uri, heeftbetrokkene_conditie, agent_conditie from GroeperingAgentOvererving"
//        .query[GroeperingAgentOverervingRow]
//        .to[List]
//
//    val groeperingRelatiesQuery: ConnectionIO[List[GroeperingRelatiesRow]] =
//      sql"select bron_uri, uri, doel_uri, richting, usagenote_nl from GroeperingRelaties"
//        .query[GroeperingRelatiesRow]
//        .to[List]
//
//    val groeperingAttribuutQuery: ConnectionIO[List[GroeperingAttribuutRow]] =
//      sql"select groepering_uri, uri, type, name, label_nl, definition_nl, kardinaliteit_min, kardinaliteit_max, readonly  from GroeperingAttributen"
//        .query[GroeperingAttribuutRow]
//        .to[List]
//
//    val groeperingClassQuery: ConnectionIO[List[GroeperingClassRow]] =
//      sql"select uri, name, label_nl, definition_nl, heeft_geometrie, heeft_locatie from GroeperingClass"
//        .query[GroeperingClassRow]
//        .to[List]
//
//    val generalInfoQuery: ConnectionIO[List[GeneralInfoRow]] =
//      sql"select Parameter, Waarde from GeneralInfo"
//        .query[GeneralInfoRow]
//        .to[List]
//  }
//
//  object validators {
//    def validURI(uri: String): Validated[URI] =
//      Try(URI.create(uri)).toEither.leftMap(t => NonEmptyChain.one(t.getMessage))
//
//    def validNes(value: String): Validated[NonEmptyString] =
//      NonEmptyString.from(value).leftMap(NonEmptyChain.one)
//
//    def emptyStringToNone(value: Option[String]): Option[String] = value.map(_.trim) match {
//      case Some("") => None
//      case v        => v
//    }
//
//    def validRichting(richting: String): Validated[Richting] = richting match {
//      case "Source -> Destination" => Richting.VanBronNaarDoel.asRight
//      case "Source <- Destination" => Richting.VanDoelNaarBron.asRight
//      case _                       => NonEmptyChain.one(s"$richting is geen geldige richting").asLeft
//    }
//
//    def validExpressie(expressie: String)(implicit validator: GRAValidator[EitherThrowable]): Validated[String] =
//      validator.validateExpression(expressie).leftMap(t => NonEmptyChain.one(t.getMessage)).map(_ => expressie)
//
//  }
//
//  object transformers {
//    val richtingTransformer: Transformer[Richting, otllib.gra.model.Richting] = {
//      case Richting.VanBronNaarDoel => otllib.gra.model.VanBronNaarDoel.INSTANCE
//      case Richting.VanDoelNaarBron => otllib.gra.model.VanDoelNaarBron.INSTANCE
//    }
//  }
//}
