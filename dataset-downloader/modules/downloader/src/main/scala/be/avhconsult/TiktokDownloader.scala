package be.avhconsult

import cats.effect.*
import cats.~>
import cats.syntax.all.*
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import fs2.concurrent.Channel
import io.circe.{ Decoder, Encoder }
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{ Headers, MediaType, Method, Request, Uri, UrlForm }
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.implicits.*
import io.circe.magnolia.derivation.encoder.semiauto.deriveMagnoliaEncoder
import io.circe.magnolia.derivation.decoder.semiauto.deriveMagnoliaDecoder
import org.http4s.client.middleware.Logger
import org.http4s.headers.Accept

import java.net.URLEncoder
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
          _            <- IO.println("dropping and creating target table")
          _            <- (dropVideoTable >> createVideoTable).transact(resources.transactor)
          _            <- IO.println("vacuum")
          _            <- resources.transactor.rawTrans.apply(vacuum)
          token        <- tokenService.getToken
          _            <- IO.println(s"token is $token")
        } yield None
      }

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
         id integer,
         video_description text,
         create_time text,
         comment_count integer,
         voice_to_text text)""".update.run >>
      sql"create index if not exists ix_id on videos(id)".update.run

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
