package be.avhconsult

import cats.effect.*
import cats.~>
import cats.syntax.all.*
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import fs2.concurrent.Channel
import io.circe.literal.*
import io.circe.{ Decoder, Encoder, Json }
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{ AuthScheme, Credentials, Headers, MediaType, Method, Request, Uri, UrlForm }
import org.http4s.circe.CirceEntityCodec.*
import io.circe.magnolia.derivation.encoder.semiauto.deriveMagnoliaEncoder
import io.circe.magnolia.derivation.decoder.semiauto.deriveMagnoliaDecoder
import org.http4s.client.middleware.Logger
import org.http4s.headers.{ Accept, Authorization }
import retry.*

import java.time.{ Instant, LocalDate }
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.*

object TiktokDownloader extends IOApp {

  sealed trait Message

  final case class ChannelMessage(nr: Long, url: String, range: String, bytes: Array[Byte]) extends Message

  final case object Done extends Message

  final case class ItemToProcess(url: String, range: String)

  final case class Resources(
      transactor: HikariTransactor[IO],
      channel: Channel[IO, Message],
      client: Client[IO],
      ioToConnectionIO: IO ~> ConnectionIO,
      tokenState: Ref[IO, Option[TokenState]]
  )

  final case class Pagination(cursor: Long, searchId: String)
  final case class VideoProgress(date: String, lastId: Option[String], lastCursor: Option[Long], lastSearchId: Option[String], downloaded: Boolean)
  final case class VideoCommentProgress(date: String, lastId: Option[String], lastCursor: Option[Long], lastSearchId: Option[String], downloaded: Boolean)
  final case class VideoResult(data: VideoDataResult)
  object VideoResult {
    implicit lazy val encoder: Encoder[VideoResult] = deriveMagnoliaEncoder[VideoResult]
    implicit lazy val decoder: Decoder[VideoResult] = deriveMagnoliaDecoder[VideoResult]
  }

  final case class VideoDataResult(cursor: Option[Long], has_more: Boolean, search_id: Option[String], videos: List[Video])
  object VideoDataResult {
    implicit lazy val encoder: Encoder[VideoDataResult] = deriveMagnoliaEncoder[VideoDataResult]
    implicit lazy val decoder: Decoder[VideoDataResult] = deriveMagnoliaDecoder[VideoDataResult]
  }

  final case class Video(
      id: Long,
      video_description: Option[String],
      // UTC Unix epoch (in seconds) of when the TikTok video was posted. (Inherited field from TNS research API)
      create_time: Long,
      comment_count: Option[Long],
      voice_to_text: Option[String]
  )
  object Video {
    implicit lazy val encoder: Encoder[Video] = deriveMagnoliaEncoder[Video]
    implicit lazy val decoder: Decoder[Video] = deriveMagnoliaDecoder[Video]
  }

  final case class VideoCommentResult(data: VideoCommentDataResult)
  object VideoCommentResult {
    implicit lazy val encoder: Encoder[VideoCommentResult] = deriveMagnoliaEncoder[VideoCommentResult]
    implicit lazy val decoder: Decoder[VideoCommentResult] = deriveMagnoliaDecoder[VideoCommentResult]
  }

  final case class VideoCommentDataResult(cursor: Option[Long], has_more: Boolean, search_id: Option[String], comments: List[VideoComment])
  object VideoCommentDataResult {
    implicit lazy val encoder: Encoder[VideoCommentDataResult] = deriveMagnoliaEncoder[VideoCommentDataResult]
    implicit lazy val decoder: Decoder[VideoCommentDataResult] = deriveMagnoliaDecoder[VideoCommentDataResult]
  }

  final case class VideoComment(
      id: Long,
      video_id: Long,
      parent_comment_id: Option[Long],
      text: String,
      // UTC Unix epoch (in seconds) of when the TikTok video was posted. (Inherited field from TNS research API)
      create_time: Long
  )
  object VideoComment {
    implicit lazy val encoder: Encoder[VideoComment] = deriveMagnoliaEncoder[VideoComment]
    implicit lazy val decoder: Decoder[VideoComment] = deriveMagnoliaDecoder[VideoComment]
  }

  override def run(args: List[String]): IO[ExitCode] =
    fs2.Stream
      .unfoldLoopEval(())(_ => doDownloads(args.headOption.getOrElse(throw new RuntimeException("geef een database naam op"))).map(v => ((), v)))
      .compile
      .drain
      .flatMap(_ => IO.println("done"))
      .as(ExitCode.Success)

  def doDownloads(database: String): IO[Option[Unit]] =
    resources(database)
      .use { resources =>
        for {
          _ <- IO.raiseError(new RuntimeException("put client secret in enviroment variable CLIENT_SECRET")).whenA(!sys.env.contains("CLIENT_SECRET"))
          _ <- IO.raiseError(new RuntimeException("put client key in enviroment variable CLIENT_KEY")).whenA(!sys.env.contains("CLIENT_KEY"))
          tokenService <- IO(TokenService.create(resources.client, resources.tokenState, sys.env("CLIENT_KEY"), sys.env("CLIENT_SECRET")))
          _            <- IO.println("creating target tables (video)")
          _            <- (createVideoProgressTable >> createVideoTable).transact(resources.transactor)
          _            <- IO.println("preparing video progress")
          _            <- prepareVideoProgress(resources.transactor)
          _ <- retryingOnAllErrors(RetryPolicies.limitRetries[IO](100), onError) {
                 for {
                   // moet naar list omdat anders de db locked blijft
                   teVerwerkenVideos <- teVerwerkenVideos
                                          .transact(resources.transactor)
                                          .compile
                                          .toList
                   _ <- teVerwerkenVideos
                          .take(1)
                          .traverse(progress => resumeVideoDownload(progress, tokenService, resources.client, resources.transactor))
                 } yield ()
               }
//          _ <- IO.println("creating target tables (video comments)")
//          _ <- (createVideoCommentProgressTable >> createVideoCommentsTable).transact(resources.transactor)
//          _ <- IO.println("preparing video comment progress")
//          _ <- prepareVideoCommentProgress(resources.transactor)
//          _ <- retryingOnAllErrors(RetryPolicies.limitRetries[IO](100), onError) {
//                 for {
//                   teVerwerkenVideoComments <- teVerwerkenVideoComments
//                                                 .transact(resources.transactor)
//                                                 .compile
//                                                 .toList
//                   _ <- teVerwerkenVideoComments
//                          .take(1)
//                          .traverse(progress => resumeVideoCommentDownload(progress, tokenService, resources.client, resources.transactor))
//                 } yield ()
//               }
        } yield None
      }

  def onError(e: Throwable, details: RetryDetails): IO[Unit] = IO.println(s"error: $e, details: $details")
  final case class VideoResumeData(progress: Option[VideoProgress], pagination: Option[Pagination])
  final case class VideoCommentResumeData(progress: Option[VideoCommentProgress], pagination: Option[Pagination])

  def resumeVideoDownload(progress: VideoProgress, tokenService: TokenService, client: Client[IO], transactor: Transactor[IO]): IO[Unit] =
    fs2.Stream
      // TODO kijken of dit wel lukt, anders resumedata terug op None zetten
      .unfoldLoopEval(VideoResumeData(Some(progress), (progress.lastCursor, progress.lastSearchId).mapN(Pagination.apply))) { resumeData =>
        for {
          (videos, newResumeData) <- fetchVideoPage(tokenService, client, resumeData)
          _                       <- IO.println(s"found ${videos.size} videos for date ${progress.date}")
          _                       <- videos.traverse(video => insertVideo(video).transact(transactor))
          _                       <- IO.println(s"updating progress")
          _                       <- videos.lastOption.traverse(video => updateProgress(progress.date, video.id.toString, newResumeData).transact(transactor))
        } yield () -> newResumeData
      }
      .metered(10.seconds)
      .compile
      .drain >> markDownloaded(progress.date).transact(transactor).void

  val regionCodes = List("NL", "BE")
  // opgelet bij resumen met een search id moet de query ook exact overeenkomen met de vorige query
  def determineConditions(resumeData: VideoResumeData): Json = resumeData match {
    case VideoResumeData(Some(videoProgress), _) if videoProgress.lastId.isEmpty =>
      json"""{
                  "not": [
                    {
                          "operation": "LT",
                          "field_name": "create_date",
                          "field_values": ${List(videoProgress.date)}
                     }
                   ],
                   "and" : [
                     {
                       "operation": "IN",
                       "field_name": "region_code",
                       "field_values": ${regionCodes}
                     }
                   ]
          }"""
    case VideoResumeData(Some(videoProgress), _) =>
      json"""{
                  "not": [
                    {
                          "operation": "LT",
                          "field_name": "create_date",
                          "field_values": ${List(videoProgress.date)}
                     }
                   ],
                   "and" : [
                     {
                       "operation": "IN",
                       "field_name": "region_code",
                       "field_values": ${regionCodes}
                     }
                   ]
          }"""
    case _ => sys.error("error in resume data determineConditions")

  }

  def determineCommentConditions(resumeData: VideoCommentResumeData): Json = resumeData match {
    case VideoCommentResumeData(Some(videoCommentProgress), _) if videoCommentProgress.lastId.isEmpty =>
      json"""{
                  "not": [
                    {
                          "operation": "LT",
                          "field_name": "create_date",
                          "field_values": ${List(videoCommentProgress.date)}
                     }
                   ],
                   "and" : [
                     {
                       "operation": "IN",
                       "field_name": "region_code",
                       "field_values": ${regionCodes}
                     }
                   ]
          }"""
    case VideoCommentResumeData(Some(videoCommentProgress), _) =>
      json"""{
                  "not": [
                    {
                          "operation": "LT",
                          "field_name": "create_date",
                          "field_values": ${List(videoCommentProgress.date)}
                     }
                   ],
                   "and" : [
                     {
                       "operation": "IN",
                       "field_name": "region_code",
                       "field_values": ${regionCodes}
                     }
                   ]
          }"""
    case _ => sys.error("error in resume data determineCommentConditions")
  }

  def fetchVideoPage(tokenService: TokenService, client: Client[IO], resumeData: VideoResumeData): IO[(List[Video], Option[VideoResumeData])] = for {
    token <- tokenService.getToken
    _     <- IO.println(s"fetching page for resume data $resumeData")
    body <- resumeData match {
              case VideoResumeData(Some(videoProgress), None) =>
                IO(
                  json"""{
              "max_count": 100,
              "start_date": ${videoProgress.date},
              "end_date": ${videoProgress.date},
              "query": ${determineConditions(resumeData)}
            }"""
                )
              case VideoResumeData(Some(videoProgress), Some(pagination)) =>
                IO(
                  json"""{
              "max_count": 100,
              "start_date": ${videoProgress.date},
              "end_date": ${videoProgress.date},
              "cursor": ${pagination.cursor},
              "search_id": ${pagination.searchId},
              "query": ${determineConditions(resumeData)}
            }"""
                )
              case _ => IO.raiseError(new RuntimeException("error in resume data"))
            }
    result <-
      client
        .run(
          Request[IO](
            Method.POST,
            Uri
              .unsafeFromString("https://open.tiktokapis.com/v2/research/video/query/")
              .withQueryParam("fields", "id,video_description,create_time,comment_count,voice_to_text")
          ).withEntity(body)
            .withHeaders(Headers(Accept(MediaType.application.json), Authorization(Credentials.Token(AuthScheme.Bearer, token))))
        )
        .use { response =>
          for {
            result <- response.as[VideoResult]
          } yield result.data.videos -> (result.data.cursor, result.data.search_id)
            .mapN(Pagination.apply)
            .flatMap(pagination => if (result.data.has_more) Some(VideoResumeData(resumeData.progress, Some(pagination))) else None)
        }
  } yield result

  def fetchVideoCommentPage(
      tokenService: TokenService,
      client: Client[IO],
      resumeData: VideoCommentResumeData
  ): IO[(List[VideoComment], Option[VideoCommentResumeData])] = for {
    token <- tokenService.getToken
    _     <- IO.println(s"fetching page for resume data $resumeData")
    body <- resumeData match {
              case VideoCommentResumeData(Some(videoCommentProgress), None) =>
                IO(
                  json"""{
              "max_count": 100,
              "start_date": ${videoCommentProgress.date},
              "end_date": ${videoCommentProgress.date},
              "query": ${determineCommentConditions(resumeData)}
            }"""
                )
              case VideoCommentResumeData(Some(videoCommentProgress), Some(pagination)) =>
                IO(
                  json"""{
              "max_count": 100,
              "start_date": ${videoCommentProgress.date},
              "end_date": ${videoCommentProgress.date},
              "cursor": ${pagination.cursor},
              "search_id": ${pagination.searchId},
              "query": ${determineCommentConditions(resumeData)}
            }"""
                )
              case _ => IO.raiseError(new RuntimeException("error in resume data"))
            }
    result <-
      client
        .run(
          Request[IO](
            Method.POST,
            Uri
              .unsafeFromString("https://open.tiktokapis.com/v2/research/video/comment/list/")
              .withQueryParam("fields", "id,video_id,text,like_count,create_time")
          ).withEntity(body)
            .withHeaders(Headers(Accept(MediaType.application.json), Authorization(Credentials.Token(AuthScheme.Bearer, token))))
        )
        .use { response =>
          for {
            result <- response.as[VideoCommentResult]
          } yield result.data.comments -> (result.data.cursor, result.data.search_id)
            .mapN(Pagination.apply)
            .flatMap(pagination => if (result.data.has_more) Some(VideoCommentResumeData(resumeData.progress, Some(pagination))) else None)
        }
  } yield result

  def resumeVideoCommentDownload(progress: VideoCommentProgress, tokenService: TokenService, client: Client[IO], transactor: Transactor[IO]): IO[Unit] =
    fs2.Stream
      // TODO kijken of dit wel lukt, anders resumedata terug op None zetten
      .unfoldLoopEval(VideoCommentResumeData(Some(progress), (progress.lastCursor, progress.lastSearchId).mapN(Pagination.apply))) { resumeData =>
        for {
          (videoComments, newResumeData) <- fetchVideoCommentPage(tokenService, client, resumeData)
          _                              <- IO.println(s"found ${videoComments.size} video comments for date ${progress.date}")
          _                              <- videoComments.traverse(video => insertVideoComment(video).transact(transactor))
          _                              <- IO.println(s"updating progress")
          _ <- videoComments.lastOption.traverse(videoComment =>
                 updateCommentProgress(progress.date, videoComment.id.toString, newResumeData).transact(transactor)
               )
        } yield () -> newResumeData
      }
      .metered(10.seconds)
      .compile
      .drain >> markCommentDownloaded(progress.date).transact(transactor).void

  def resources(dbName: String): Resource[IO, Resources] =
    for {
      transactor       <- transactor(dbName)
      channel          <- Resource.eval(Channel.bounded[IO, Message](50))
      client           <- EmberClientBuilder.default[IO].withIdleConnectionTime(30.seconds).withTimeout(10.seconds).build
      ioToConnectionIO <- WeakAsync.liftK[IO, ConnectionIO]
      tokenState       <- Resource.eval(Ref.of[IO, Option[TokenState]](None))
    } yield Resources(transactor, channel, Logger(logHeaders = true, logBody = true, logAction = Some(IO.println))(client), ioToConnectionIO, tokenState)

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

  val dropVideoTable: ConnectionIO[Int] =
    sql"drop table if exists videos".update.run

  val createVideoTable: ConnectionIO[Int] =
    sql"""create table if not exists videos(
         id integer primary key,
         video_description text,
         create_time text,
         comment_count integer,
         voice_to_text text)""".update.run >>
      sql"create index if not exists ix_id on videos(id)".update.run

  val createVideoCommentsTable: ConnectionIO[Int] =
    sql"""create table if not exists video_comments(
          id integer primary key, video_id integer, comment_text text, parent_comment_id integer, create_time text)""".update.run >>
      sql"create index if not exists ix_id on video_comments(id)".update.run >>
      sql"create index if not exists ix_video_id on video_comments(video_id)".update.run

  val createVideoProgressTable: ConnectionIO[Int] =
    sql"""create table if not exists video_progress(video_date text primary key, last_id text, last_cursor integer, last_search_id text, downloaded boolean)""".update.run

  val createVideoCommentProgressTable: ConnectionIO[Int] =
    sql"""create table if not exists video_comment_progress(video_date text primary key, last_id text, last_cursor integer, last_search_id text, downloaded boolean)""".update.run

  val teVerwerkenVideos: fs2.Stream[ConnectionIO, VideoProgress] =
    sql"""select video_date, last_id, last_cursor, last_search_id, downloaded from video_progress where downloaded = false order by video_date asc"""
      .query[VideoProgress]
      .stream

  val teVerwerkenVideoComments: fs2.Stream[ConnectionIO, VideoCommentProgress] =
    sql"""select p.video_date, p.last_id, p.last_cursor, p.last_search_id, p.downloaded from video_comment_progress p, video_progress v where p.downloaded = false and p.video_date = v.video_date and v.downloaded = true order by p.video_date asc"""
      .query[VideoCommentProgress]
      .stream

  def updateProgress(videoDate: String, lastId: String, resumeData: Option[VideoResumeData]): ConnectionIO[Int] = resumeData match {
    case Some(VideoResumeData(_, Some(Pagination(cursor, searchId)))) =>
      sql"update video_progress set last_id = $lastId, last_cursor = ${cursor}, last_search_id = ${searchId} where video_date = $videoDate".update.run
    case _ =>
      sql"update video_progress set last_id = $lastId, last_cursor = null, last_search_id = null where video_date = $videoDate".update.run
  }

  def updateCommentProgress(videoDate: String, lastId: String, resumeData: Option[VideoCommentResumeData]): ConnectionIO[Int] = resumeData match {
    case Some(VideoCommentResumeData(_, Some(Pagination(cursor, searchId)))) =>
      sql"update video_comment_progress set last_id = $lastId, last_cursor = ${cursor}, last_search_id = ${searchId} where video_date = $videoDate".update.run
    case _ =>
      sql"update video_comment_progress set last_id = $lastId, last_cursor = null, last_search_id = null where video_date = $videoDate".update.run
  }

  def markDownloaded(videoDate: String): ConnectionIO[Int] =
    sql"update video_progress set downloaded = true where video_date = $videoDate".update.run

  def markCommentDownloaded(videoDate: String): ConnectionIO[Int] =
    sql"update video_comment_progress set downloaded = true where video_date = $videoDate".update.run

  def insertVideo(video: Video): ConnectionIO[Int] =
    sql"""insert or ignore into videos(id, video_description, create_time, comment_count, voice_to_text)
         values(${video.id}, ${video.video_description}, ${Instant
      .ofEpochSecond(video.create_time)
      .toString}, ${video.comment_count.getOrElse(0L)}, ${video.voice_to_text})""".update.run

  def insertVideoComment(videoComment: VideoComment): ConnectionIO[Int] =
    sql"""insert or ignore into video_comment(id, video_id, comment_text, parent_comment_id, create_time)
         values(${videoComment.id}, ${videoComment.video_id}, ${videoComment.text}, ${videoComment.parent_comment_id}, ${Instant
      .ofEpochSecond(videoComment.create_time)
      .toString})""".update.run

  def prepareVideoProgress(transactor: Transactor[IO]): IO[Unit] = {
    val start = LocalDate.of(2022, 2, 1)
    val end   = LocalDate.of(2024, 12, 1)
    fs2.Stream
      .unfoldEval(start)(date => {
        if (date.isBefore(end)) {
          val dateString = s"${date.getYear}${"%02d".format(date.getMonthValue)}${"%02d".format(date.getDayOfMonth)}"
          sql"""insert or ignore into video_progress(video_date, last_id, downloaded) values($dateString, null, false)""".update.run
            .transact(transactor)
            .as(Some(() -> date.plusDays(1)))
        } else {
          IO(None)
        }
      })
      .compile
      .drain
  }

  def prepareVideoCommentProgress(transactor: Transactor[IO]): IO[Unit] = {
    val start = LocalDate.of(2022, 2, 1)
    val end   = LocalDate.of(2024, 12, 1)
    fs2.Stream
      .unfoldEval(start)(date => {
        if (date.isBefore(end)) {
          val dateString = s"${date.getYear}${"%02d".format(date.getMonthValue)}${"%02d".format(date.getDayOfMonth)}"
          sql"""insert or ignore into video_comment_progress(video_date, last_id, downloaded) values($dateString, null, false)""".update.run
            .transact(transactor)
            .as(Some(() -> date.plusDays(1)))
        } else {
          IO(None)
        }
      })
      .compile
      .drain
  }

  val vacuum: ConnectionIO[Int] =
    sql"vacuum".update.run

  final case class TokenState(token: String, expires: Instant)

  final case class TokenResult(access_token: String, expires_in: Long)

  object TokenResult {
    implicit lazy val encoder: Encoder[TokenResult] = deriveMagnoliaEncoder[TokenResult]
    implicit lazy val decoder: Decoder[TokenResult] = deriveMagnoliaDecoder[TokenResult]
  }

  trait TokenService {
    def getToken: IO[String]
  }

  object TokenService {
    def create(client: Client[IO], state: Ref[IO, Option[TokenState]], clientKey: String, clientSecret: String): TokenService = new TokenService {
      override def getToken: IO[String] = (state.get, Clock[IO].realTimeInstant).flatMapN {
        case (None, _)                                                                                      => fetchToken
        case (Some(TokenState(token, expires)), now) if now.plus(10L, ChronoUnit.MINUTES).isBefore(expires) => IO(token)
        case (Some(_), _)                                                                                   => fetchToken
      }

      private def fetchToken: IO[String] = {
        IO.println(s"must fetch new token") >>
          client
            .run(
              Request[IO](
                Method.POST,
                Uri.unsafeFromString("https://open.tiktokapis.com/v2/oauth/token/"),
                headers = Headers(Accept(MediaType.application.json))
              )
                .withEntity(
                  UrlForm("grant_type" -> "client_credentials", "client_key" -> clientKey, "client_secret" -> clientSecret)
                )
                .withHeaders(Headers("Content-Type" -> "application/x-www-form-urlencoded"))
            )
            .use { response =>
              for {
                tokenResult <- response.as[TokenResult]
                now         <- Clock[IO].realTimeInstant
                _           <- state.set(Some(TokenState(tokenResult.access_token, now.plusSeconds(tokenResult.expires_in))))

              } yield tokenResult.access_token
            }
      }
    }
  }
}
