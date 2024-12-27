package otllib.gra.api.sqlite

object GRAReader {}
//import be.wegenenverkeer.infra.infrastructure.stack.either.EitherThrowable
//import otllib.gra.api.*
//import otllib.gra.api.sqlite.queries.*
//import otllib.gra.api.sqlite.types.*
//import otllib.gra.api.sqlite.validators.*
//import otllib.gra.api.model.*
//import cats.syntax.all.*
//import cats.Parallel
//import cats.data.NonEmptyChain
//import cats.effect.Async
//import doobie.implicits.*
//import eu.timepit.refined.types.all.*
//import otllib.gra.api.validation.{ ExpressionParser, GRAValidator }
//
//import java.io.File
//
//trait GRAReader[F[_]] {
//
//  /** uitlezen van een GRA model uit een sqlite file
//    * @param file
//    *   de sqlite database die moet uitgelezen worden
//    * @return
//    *   het GRAModel
//    */
//  def fromFile(file: File): F[GRAModel]
//
//  /** uitlezen van een GRA model uit een sqlite file in het classpath
//    * @param name
//    *   de naam van de sqlite database in het classpath die moet uitgelezen worden
//    * @return
//    *   het GRAModel
//    */
//  def fromClassPath(name: String): F[GRAModel]
//}
//
//object GRAReader {
//  implicit val validator: GRAValidator[EitherThrowable] = GRAValidator.create(ExpressionParser.create)
//
//  /** aanmaken van een GRAReader die alle effecten sequentieel uitvoert */
//  def createSequential[F[_]](implicit F: Async[F]): GRAReader[F] =
//    create[F](F, Parallel.identity[F])
//
//  /** aanmaken van een GRAReader  die in staat is om effecten parallel uit te voeren */
//  def create[F[_]](implicit F: Async[F], P: Parallel[F]): GRAReader[F] = new GRAReader[F] {
//
//    override def fromFile(file: File): F[GRAModel] =
//      withName(file.getAbsolutePath)
//
//    override def fromClassPath(name: String): F[GRAModel] =
//      withName(s":resource:$name")
//
//    private def withName(name: String): F[GRAModel] = {
//      transactor[F](name)
//        .use { xa =>
//          (groeperingStartQuery.transact(xa),
//           groeperingOpbouwQuery.transact(xa),
//           groeperingAttribuutOverervingQuery.transact(xa),
//           groeperingAgentOverervingQuery.transact(xa),
//           groeperingRelatiesQuery.transact(xa),
//           groeperingAttribuutQuery.transact(xa),
//           groeperingClassQuery.transact(xa),
//           generalInfoQuery.transact(xa)
//          ).parMapN { case (start, opbouw, attribuutOvererving, agentOvererving, relaties, attributen, groeperingen, generalInfos) =>
//            (
//              start.parTraverse(validateGroeperingStart).map(_.groupBy(_.id).view.mapValues(_.head).toMap),
//              opbouw.parTraverse(validateGroeperingOpbouw).map(_.groupBy(_.id).view.mapValues(_.toSet).toMap),
//              attribuutOvererving.parTraverse(validateGroeperingAttribuutOvererving).map(_.groupBy(_.id).view.mapValues(_.toSet).toMap),
//              agentOvererving.parTraverse(validateGroeperingAgentOvererving).map(_.groupBy(_.id).view.mapValues(_.toSet).toMap),
//              relaties.parTraverse(validateGroeperingRelatie),
//              attributen.parTraverse(validateGroeperingAttribuut).map(_.groupBy(_.groeperingURI).view.mapValues(_.toSet).toMap),
//              groeperingen.parTraverse(validateGroeperingClass).map(_.groupBy(_.groeperingURI).view.mapValues(_.head).toMap),
//              validateGeneralInfos(generalInfos)
//            )
//              .parMapN(GRAModel.apply)
//          }
//        }
//        .recoverWith { case t => GRAReaderFout(NonEmptyChain.one(t.getMessage)).raiseError[F, Validated[GRAModel]] }
//        .flatMap {
//          case Right(value) => value.pure[F]
//          case Left(fouten) => GRAReaderFout(fouten).raiseError[F, GRAModel]
//        }
//    }
//  }
//
//  def validateGroeperingStart(row: GroeperingStartRow): Validated[GroeperingStart] = row match {
//    case (id, groeperingUri, startUri, startConditie) =>
//      (id.asRight, validURI(groeperingUri), emptyStringToNone(startUri).traverse(validURI), emptyStringToNone(startConditie).traverse(validExpressie))
//        .parMapN(GroeperingStart.apply)
//  }
//
//  def validateGroeperingOpbouw(row: GroeperingOpbouwRow): Validated[GroeperingOpbouw] = row match {
//    case (id, bronUri, relatieUri, relatieConditie, doelUri, doelConditie, richting, geometrieCollection) =>
//      (id.asRight,
//       validURI(bronUri),
//       validURI(relatieUri),
//       emptyStringToNone(relatieConditie).traverse(validExpressie),
//       validURI(doelUri),
//       emptyStringToNone(doelConditie).traverse(validExpressie),
//       validRichting(richting),
//       geometrieCollection.asRight
//      ).parMapN(GroeperingOpbouw.apply)
//  }
//
//  def validateGroeperingAttribuutOvererving(row: GroeperingAttribuutOverervingRow): Validated[GroeperingAttribuutOvererving] = row match {
//    case (id, bronUri, overervingOmschrijving, overervingLevel) =>
//      (id.asRight, validURI(bronUri), emptyStringToNone(overervingOmschrijving).traverse(validExpressie), emptyStringToNone(overervingLevel).asRight)
//        .parMapN(GroeperingAttribuutOvererving.apply)
//  }
//
//  def validateGroeperingAgentOvererving(row: GroeperingAgentOverervingRow): Validated[GroeperingAgentOvererving] = row match {
//    case (id, uri, heeftBetrokkeneConditie, agentConditie) =>
//      (id.asRight,
//       validURI(uri),
//       emptyStringToNone(heeftBetrokkeneConditie).traverse(validExpressie),
//       emptyStringToNone(agentConditie).traverse(validExpressie)
//      ).parMapN(GroeperingAgentOvererving.apply)
//  }
//
//  def validateGroeperingRelatie(row: GroeperingRelatiesRow): Validated[GroeperingRelatie] = row match {
//    case (bronUri, relatieUri, doelUri, richting, usageNote) =>
//      (validURI(bronUri), validURI(relatieUri), validURI(doelUri), validRichting(richting), emptyStringToNone(usageNote).asRight)
//        .parMapN(GroeperingRelatie.apply)
//  }
//
//  def validateGroeperingAttribuut(row: GroeperingAttribuutRow): Validated[GroeperingAttribuut] = row match {
//    case (groeperingUri, attribuutUri, typeUri, naam, label, definitie, kardinaliteitMin, kardinaliteitMax, readonly) =>
//      (validURI(groeperingUri),
//       validURI(attribuutUri),
//       validURI(typeUri),
//       naam.asRight,
//       label.asRight,
//       emptyStringToNone(definitie).asRight,
//       kardinaliteitMin.asRight,
//       kardinaliteitMax.asRight,
//       readonly.asRight
//      ).parMapN(GroeperingAttribuut.apply)
//  }
//
//  def validateGroeperingClass(row: GroeperingClassRow): Validated[GroeperingClass] = row match {
//    case (groeperingUri, naam, label, definitie, heeftGeometrie, heeftLocatie) =>
//      (validURI(groeperingUri), naam.asRight, label.asRight, emptyStringToNone(definitie).asRight, heeftGeometrie.asRight, heeftLocatie.asRight)
//        .parMapN(GroeperingClass.apply)
//  }
//
//  def validateGeneralInfos(rows: List[GeneralInfoRow]): Validated[Metadata] = {
//    val naam: Validated[NonEmptyString] = rows
//      .collectFirst {
//        case (key, value) if key == METADATA_KEY_FILENAME.value => validNes(value)
//      }
//      .fold(NonEmptyChain.of("meta gegeven Filename kon niet gevonden worden").asLeft[NonEmptyString])(identity)
//    val versie: Validated[NonEmptyString] = rows
//      .collectFirst {
//        case (key, value) if key == METADATA_KEY_VERSIE.value => validNes(value)
//      }
//      .fold(NonEmptyChain.of("meta gegeven Version kon niet gevonden worden").asLeft[NonEmptyString])(identity)
//    val rest: Validated[Map[NonEmptyString, NonEmptyString]] = rows
//      .filterNot { case (key, _) => key == METADATA_KEY_FILENAME.value || key == METADATA_KEY_VERSIE.value }
//      .parTraverse { case (key, value) => (validNes(key), validNes(value)).parTupled }
//      .map(_.toMap)
//    (naam, versie, rest).parMapN(Metadata.apply)
//  }
//}
