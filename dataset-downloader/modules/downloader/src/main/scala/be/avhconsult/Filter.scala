package be.avhconsult

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import fs2.concurrent.Channel
import org.apache.tika.Tika
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{ Metadata, Property, TikaCoreProperties }
import org.jsoup.Jsoup
import org.netpreserve.jwarc.{ MediaType, WarcReader, WarcResponse }

import java.net.{ URI, URL }
import java.nio.charset.{ Charset, StandardCharsets }
import java.security.MessageDigest
import java.util.Optional
import java.util.concurrent.{ ExecutorService, Executors }
import scala.concurrent.ExecutionContext
import scala.util.matching.Regex

object Filter extends IOApp {

  sealed trait Message
  final case class ChannelMessage(
      nr: Long,
      url: String,
      urlp1: String,
      urlp2: String,
      urlp3: String,
      host: String,
      mime: String,
      timestamp: String,
      content: String,
      originalContent: String,
      linkPercentage: Double,
      contentHash: String,
      languages: Option[String],
      totalNrHits: Int,
      distinctNrHits: Int,
      matches: String,
      matchesWords: String,
      politicalParty: Boolean,
      newsOutlet: Boolean
  ) extends Message
  final case object EmptyMessage extends Message
  final case object Done extends Message
  final case class ItemToProcess(
      url: String,
      urlp1: String,
      urlp2: String,
      urlp3: String,
      mime: String,
      charset: Option[String],
      timestamp: String,
      bytes: Array[Byte],
      languages: Option[String]
  )
  final case class Resources(
      sourceTransactor: HikariTransactor[IO],
      targetTransactor: HikariTransactor[IO],
      uriMappingTransactor: HikariTransactor[IO],
      channel: Channel[IO, Message],
      tika: Tika,
      executionContext: ExecutionContext
  )
  final case class Mapping(politicalParty: Boolean, newsOutlet: Boolean)

  // should we search on word boundaries?  ie """\b(migrant|...)\b""".r
  // or only start on word boundaries?  ie """\b(migrant|...)""".r
  val pattern =
    """\b(migrant|immigrant|emigrant|migratie|immigratie|vluchteling|oorlogsvluchteling|ontheemde|vluchtende bevolking|verspreide bevolking|herplaatste bevolking|asielzoeker|buitenlander|migration|immigration|refugee|war refugees|displaced people|fleeing population|dispersed population|relocated population|asylum seeker|diaspora|expatriate|expat|émigrant|réfugié|réfugiés de guerre|personnes déplacées|population fuyante|population dispersée|personne relocalisée|demandeur d'asile|étranger|expatrié)""".r

  override def run(args: List[String]): IO[ExitCode] = {
    val source = args.headOption.getOrElse(throw new RuntimeException("geef een source database naam op"))
    val target = args.drop(1).headOption.getOrElse(throw new RuntimeException("geef een target database naam op"))

    fs2.Stream
      .unfoldLoopEval(())(_ => Clock[IO].timed(process(source, target)).flatMap { case (duration, v) => IO.println(s"execution took $duration").as(((), v)) })
      .compile
      .drain
      .flatMap(_ => IO.println("done"))
      .as(ExitCode.Success)
  }

  def process(source: String, target: String): IO[Option[Unit]] =
    resources(source, target)
      .use { resources =>
        for {
          _ <- IO.println("dropping and creating target table")
          _ <- (dropTargetTable >> createTargetTable).transact(resources.targetTransactor)
          _ <- IO.println("vacuum")
          _ <- resources.targetTransactor.rawTrans.apply(vacuum)
          _ <- IO.println("fetching uri mapping")
          uriMapping <-
            readMappings
              .transact(resources.uriMappingTransactor)
              .compile
              .toList
              .map(_.groupMapReduce { case (uri, _, _) => uri } { case (_, politicalParty, newsOutlet) => Mapping(politicalParty, newsOutlet) }((f, _) => f))
          subscriberDone <- Deferred[IO, Unit]
          _ <- {
            val publisher: fs2.Stream[IO, Unit] =
              resources.sourceTransactor.transP
                .apply(getHtmlItemsToProcess)
                .zipWithIndex
                .parEvalMap(20) { case (itemToProcess, number) =>
                  processHtmlItemToProcess(number, itemToProcess, uriMapping, resources.executionContext)
                    .flatMap(_.traverse(resources.channel.send).void)
                    .handleErrorWith(t => IO.println(s"error processing # ${number} ${itemToProcess.url} with mime ${itemToProcess.mime} ${t.getMessage}"))
                } ++ resources.sourceTransactor.transP
                .apply(getNonHtmlItemsToProcess)
                .zipWithIndex
                .parEvalMap(20) { case (itemToProcess, number) =>
                  processNonHtmlItemToProcess(number, itemToProcess, uriMapping, resources.tika, resources.executionContext)
                    .flatMap(_.traverse(resources.channel.send).void)
                    .handleErrorWith(t => IO.println(s"error processing # ${number} ${itemToProcess.url} with mime ${itemToProcess.mime} ${t.getMessage}"))
                } ++ fs2.Stream.eval(IO.println("done sending all messages")) ++ fs2.Stream.eval(resources.channel.send(Done).void)
            val subscriber: fs2.Stream[IO, Unit] =
              resources.channel.stream.evalMap {
                case EmptyMessage => IO.println(s"skipping empty message")
                case msg: ChannelMessage =>
                  for {
                    _ <- sql"select 1 from hits where content_hash = ${msg.contentHash}".query[Int].option.transact(resources.targetTransactor).flatMap {
                           case Some(_) => IO.println(s"skipping message # ${msg.nr} content hash ${msg.contentHash} already found")
                           case None =>
                             sql"""insert into hits(
                                  url,
                                  urlp1,
                                  urlp2,
                                  urlp3,
                                  host,
                                  mime,
                                  timestamp,
                                  content,
                                  orig_content,
                                  link_percentage,
                                  content_hash,
                                  languages,
                                  total_nr_hits,
                                  distinct_nr_hits,
                                  matches,
                                  matches_words,
                                  political_party,
                                  news_outlet,
                                  relevant,
                                  disinformation)
                                  values (
                                  ${msg.url},
                                  ${msg.urlp1},
                                  ${msg.urlp2},
                                  ${msg.urlp3},
                                  ${msg.host},
                                  ${msg.mime},
                                  ${msg.timestamp},
                                  ${msg.content},
                                  ${msg.originalContent},
                                  ${msg.linkPercentage},
                                  ${msg.contentHash},
                                  ${msg.languages},
                                  ${msg.totalNrHits},
                                  ${msg.distinctNrHits},
                                  ${msg.matches},
                                  ${msg.matchesWords},
                                  ${msg.politicalParty},
                                  ${msg.newsOutlet},
                                  null,
                                  null)""".update.run
                               .transact(resources.targetTransactor)
                               .void
                               .handleErrorWith(t => IO.println(s"problem persisting message # ${msg.nr} ${msg.url} ${t.getMessage}"))
                         }
                  } yield ()
                case Done => IO.println("received done message, can release subscriber") >> subscriberDone.complete(()).void
              }
            val finalize =
              fs2.Stream.eval {
                subscriberDone.get >>
                  IO.println("subscriber is done, can release channel") >>
                  resources.channel.close.void
              }
            fs2.Stream(publisher, subscriber, finalize).parJoinUnbounded.compile.drain
          }

        } yield None
      }

  private def findMapping(item: ItemToProcess, uriMappings: Map[String, Mapping]): (String, Mapping) = {
    // check for url matches
    val host: String = URI.create(item.url).getHost.toLowerCase
    uriMappings
      .find { case (key, _) =>
        key == host || host.endsWith(key)
      }
      .getOrElse(("ongekend", Mapping(false, false)))
  }

  private def matches(matchesContent: List[Regex.Match]): (Set[String], String, String) = {
    val matchSet           = matchesContent.map(_.group(1)).toSet
    val matchesString      = matchesContent.map(m => s"${m.start}-${m.end}").mkString(",")
    val matchesWordsString = matchesContent.map(_.matched).mkString(",")
    (matchSet, matchesString, matchesWordsString)
  }

  def processHtmlItemToProcess(
      number: Long,
      item: ItemToProcess,
      uriMappings: Map[String, Mapping],
      executionContext: ExecutionContext
  ): IO[Option[Message]] =
    for {
      _ <- IO.println(s"processed # ${number} messages").whenA(number % 1000 == 0)
      result <- item.mime match {
                  case "text/html" =>
                    fs2.io
                      .toInputStreamResource(fs2.Stream.chunk[IO, Byte](fs2.Chunk.array(item.bytes)))
                      .flatMap(is => Resource.fromAutoCloseable(IO(new WarcReader(is))))
                      .use { reader =>
                        IO.blocking {
                          optionalToOption(reader.next()).flatMap {
                            case record: WarcResponse if record.contentType.base == MediaType.HTTP =>
                              val doc = Jsoup
                                .parse(record.http.bodyDecoded.stream, item.charset.orElse(determineCharset(record)).getOrElse("UTF-8"), record.target)
                              doc.select("script").remove()
                              val originalContent = doc.toString
                              // remove certain elements that contain links and navigations
                              doc
                                .select(
                                  "nav, header, footer, form, img, svg, aside, [role=navigation], [role=banner], [role=menu], [role=form], [role=search], [role=button], [class~=(?i)(footer|navbar|articles-list|list-group)]"
                                )
                                .remove()
                              // delete hrefs and other attributes in order to avoid hits in href attributes
                              doc.select("a").forEach { link =>
                                link.clearAttributes()
                                ()
                              }
                              val content: String     = doc.body.text
                              val contentHash: String = bytesToHex(MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8)))
                              doc.select("a").remove()
                              val contentWithoutLinks: String = doc.body.text
                              val linkPercentageText: Double  = 1.0 - contentWithoutLinks.length.toDouble / content.length.toDouble

                              val matchesContent             = pattern.findAllMatchIn(content).toList
                              val matchesContentWithoutlinks = pattern.findAllMatchIn(contentWithoutLinks).toList

                              if (matchesContent.nonEmpty && matchesContentWithoutlinks.nonEmpty) {

                                val (matchSet, matchesString, matchesWordsString) = matches(matchesContent)
                                val (key, mapping)                                = findMapping(item, uriMappings)

                                Some(
                                  ChannelMessage(
                                    nr = number,
                                    url = item.url,
                                    urlp1 = item.urlp1,
                                    urlp2 = item.urlp2,
                                    urlp3 = item.urlp3,
                                    host = key,
                                    mime = item.mime,
                                    timestamp = item.timestamp,
                                    content = content,
                                    originalContent = originalContent,
                                    linkPercentage = linkPercentageText,
                                    contentHash = contentHash,
                                    languages = item.languages,
                                    totalNrHits = matchesContent.size,
                                    distinctNrHits = matchSet.size,
                                    matches = matchesString,
                                    matchesWords = matchesWordsString,
                                    politicalParty = mapping.politicalParty,
                                    newsOutlet = mapping.newsOutlet
                                  )
                                )
                              } else {
                                None
                              }

                            case _ => None
                          }
                        }.evalOn(executionContext)
                      }
                  case _ => IO(None)
                }
    } yield result

  def processNonHtmlItemToProcess(
      number: Long,
      item: ItemToProcess,
      uriMappings: Map[String, Mapping],
      tika: Tika,
      executionContext: ExecutionContext
  ): IO[Option[Message]] =
    for {
      _ <- IO.println(s"processed # ${number} messages").whenA(number % 1000 == 0)
      result <- item.mime match {
                  case mime
                      if mime.contains("xml") || mime.contains("json") || mime.contains("zip") || mime.contains("image") || mime.contains("audio") || mime
                        .contains("calendar") =>
                    IO.println(s"not processing non-html nr $number with mime type ${item.mime} for ${item.url}") >> IO(Some(EmptyMessage))
                  case mime =>
                    IO.println(s"processing non-html nr $number with mime type ${item.mime} for ${item.url}") >>
                      Resource
                        .fromAutoCloseable(IO {
                          val metadata = new Metadata()
                          metadata.set(TikaCoreProperties.CONTENT_TYPE_USER_OVERRIDE, mime)
                          TikaInputStream.get(item.bytes, metadata)
                        })
                        .use { is =>
                          IO.blocking {
                            val content: String     = tika.parseToString(is, new Metadata(), 10 * 1024 * 1024)
                            val contentHash: String = bytesToHex(MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8)))

                            val matchesContent = pattern.findAllMatchIn(content).toList

                            if (matchesContent.nonEmpty) {

                              val (matchSet, matchesString, matchesWordsString) = matches(matchesContent)
                              val (key, mapping)                                = findMapping(item, uriMappings)

                              Some(
                                ChannelMessage(
                                  nr = number,
                                  url = item.url,
                                  urlp1 = item.urlp1,
                                  urlp2 = item.urlp2,
                                  urlp3 = item.urlp3,
                                  host = key,
                                  mime = item.mime,
                                  timestamp = item.timestamp,
                                  content = content,
                                  originalContent = "",
                                  linkPercentage = 0,
                                  contentHash = contentHash,
                                  languages = item.languages,
                                  totalNrHits = matchesContent.size,
                                  distinctNrHits = matchSet.size,
                                  matches = matchesString,
                                  matchesWords = matchesWordsString,
                                  politicalParty = mapping.politicalParty,
                                  newsOutlet = mapping.newsOutlet
                                )
                              )
                            } else {
                              None
                            }
                          }.evalOn(executionContext)
                        }
                }
    } yield result

  private def determineCharset(response: WarcResponse): Option[String] = {
    optionalToOption(
      response.http.headers
        .first("Content-Type")
        .map((contentType: String) => contentType.split(";"))
        .filter((parts: Array[String]) => parts.length > 1)
        .map((parts: Array[String]) => parts(1).split("="))
        .filter((parts: Array[String]) => parts.length > 1)
        .map((parts: Array[String]) => parts(1))
    )
  }

  private def optionalToOption[T](value: Optional[T]): Option[T] = {
    if (value.isPresent) {
      Some(value.get())
    } else {
      None
    }
  }

  val getHtmlItemsToProcess: fs2.Stream[ConnectionIO, ItemToProcess] =
    sql"select url, urlp1, urlp2, urlp3, mime, charset, timestamp, downloaded_content, languages from download_progress where downloaded = 1 and mime = 'text/html'"
      .query[ItemToProcess]
      .stream

  val getNonHtmlItemsToProcess: fs2.Stream[ConnectionIO, ItemToProcess] =
    sql"select url, urlp1, urlp2, urlp3, mime, charset, timestamp, downloaded_content, languages from download_progress where downloaded = 1 and mime != 'text/html'"
      .query[ItemToProcess]
      .stream

  val dropTargetTable: ConnectionIO[Int] =
    sql"drop table if exists hits".update.run

  val createTargetTable: ConnectionIO[Int] =
    sql"""create table if not exists hits(
         url text,
         urlp1 text,
         urlp2 text,
         urlp3 text,
         host text,
         mime text,
         timestamp text,
         content text,
         orig_content text,
         link_percentage double,
         content_hash text,
         languages text,
         total_nr_hits,
         distinct_nr_hits,
         matches text,
         matches_words text,
         political_party boolean,
         news_outlet boolean,
         relevant boolean,
         disinformation boolean)""".update.run >>
      sql"create index if not exists ix_content_hash on hits(content_hash)".update.run

  val readMappings: fs2.Stream[ConnectionIO, (String, Boolean, Boolean)] =
    sql"select uri, political_party, news_outlet from uri_mapping".query[(String, Boolean, Boolean)].stream

  val vacuum: ConnectionIO[Int] =
    sql"vacuum".update.run

  def resources(source: String, target: String): Resource[IO, Resources] =
    for {
      sourceTransactor     <- transactor(source)
      targetTransactor     <- transactor(target)
      uriMappingTransactor <- transactor("uri-mapping.sqlite")
      channel              <- Resource.eval(Channel.bounded[IO, Message](50))
      tika                 <- Resource.pure(new Tika())
      pool                 <- Resource.make(IO.blocking(Executors.newFixedThreadPool(20)))(es => IO.blocking(es.shutdown())).map(ExecutionContext.fromExecutor)
    } yield Resources(sourceTransactor, targetTransactor, uriMappingTransactor, channel, tika, pool)

  def transactor(name: String): Resource[IO, HikariTransactor[IO]] =
    for {
      ce <- ExecutionContexts.cachedThreadPool[IO] // our connect EC
      xa <- HikariTransactor.newHikariTransactor[IO](
              "org.sqlite.JDBC", // driver classname
              s"jdbc:sqlite:$name", // connect URL
              "", // username
              "", // password
              ce // await connection here
            )
    } yield xa

  def bytesToHex(hash: Array[Byte]): String = {
    val hexString: StringBuffer = new StringBuffer
    for (i <- hash.indices) {
      val hex: String = Integer.toHexString(0xff & hash(i))
      if (hex.length == 1) {
        hexString.append('0')
      }
      hexString.append(hex)
    }
    hexString.toString
  }
}
