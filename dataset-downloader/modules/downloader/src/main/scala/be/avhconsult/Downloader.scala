package be.avhconsult
import cats.effect.*
import cats.~>
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import fs2.concurrent.Channel
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{ Method, Uri, Request }

import scala.concurrent.duration.*

object Downloader extends IOApp {

  sealed trait Message
  final case class ChannelMessage(nr: Long, url: String, range: String, bytes: Array[Byte]) extends Message
  final case object Done extends Message
  final case class ItemToProcess(url: String, range: String)
  final case class Resources(transactor: HikariTransactor[IO], channel: Channel[IO, Message], client: Client[IO], ioToConnectionIO: IO ~> ConnectionIO)

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
          _              <- IO.println("querying database for urls to download")
          subscriberDone <- Deferred[IO, Unit]
          _ <- {
            val publisher: fs2.Stream[IO, Unit] =
              fs2.Stream
                .evalSeq(downloadUrlQuery.transact(resources.transactor))
                .metered(100.milliseconds)
                .zipWithIndex
                .parEvalMap(10) { case (itemToProcess, number) =>
                  (for {
                    bytes <- processUrl(number, itemToProcess.url, itemToProcess.range, resources.client)
                    _     <- resources.channel.send(ChannelMessage(number, itemToProcess.url, itemToProcess.range, bytes))
                  } yield ()).handleErrorWith(t =>
                    IO.println(s"error downloading # ${number} ${itemToProcess.url} with range ${itemToProcess.range} ${t.getMessage}")
                  )
                } ++ fs2.Stream.eval(IO.println("done sending all messages")) ++ fs2.Stream.eval(resources.channel.send(Done).void)
            val subscriber: fs2.Stream[IO, Unit] =
              resources.channel.stream.evalMap {
                case msg: ChannelMessage =>
                  IO.println(s"persisting downloaded bytes for # ${msg.nr} ${msg.url} with range ${msg.range}") >>
                    persistDownloadedContent(msg, resources.ioToConnectionIO)
                      .transact(resources.transactor)
                      .void
                      .handleErrorWith(t => IO.println(s"problem persisting bytes for # ${msg.nr} ${msg.url} with range ${msg.range} ${t.getMessage}"))
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
          unprocessed <- countQuery.transact(resources.transactor)
          _           <- IO.println(s"${unprocessed} urls left to download")
          result      <- if (unprocessed == 0) IO(None) else IO.sleep(60.seconds).map(_ => Some(()))
        } yield result
      }

  def processUrl(nr: Long, url: String, range: String, client: Client[IO]): IO[Array[Byte]] =
    for {
      chunk <-
        client
          .stream(
            Request[IO](Method.GET, Uri.unsafeFromString(url))
              .withHeaders("Range" -> s"bytes=${range}", "Accept-Encoding" -> "gzip", "agent" -> "cc-ou-nl")
          )
          .flatMap {
            case response if response.status.code == 206 => response.body.chunks
            case response                                => fs2.Stream.raiseError[IO](new RuntimeException(s"unexpected response ${response.status.code}"))
          }
          .compile
          .foldMonoid
      _ <-
        IO.println(s"downloaded # ${nr} ${url} with range ${range} and size ${chunk.size} on thread ${Thread.currentThread().getName}")
    } yield chunk.toArray

  val downloadUrlQuery: ConnectionIO[List[ItemToProcess]] =
    sql"select data_url, range from download_progress where downloaded = 0"
      .query[ItemToProcess]
      .to[List]

  val countQuery: ConnectionIO[Int] =
    sql"select count(*) from download_progress where downloaded = 0"
      .query[Int]
      .unique

  def persistDownloadedContent(message: ChannelMessage, ioToConnectionIO: IO ~> ConnectionIO): ConnectionIO[Int] = {
    if (new String(message.bytes).contains("Please reduce your request rate")) {
      ioToConnectionIO(IO.println("Please reduce your request rate").as(0))
    } else {
      sql"update download_progress set downloaded_content = ${message.bytes}, downloaded = 1 where data_url = ${message.url} and range = ${message.range}".update.run
    }
  }

  def resources(dbName: String): Resource[IO, Resources] =
    for {
      transactor       <- transactor(dbName)
      channel          <- Resource.eval(Channel.bounded[IO, Message](50))
      client           <- EmberClientBuilder.default[IO].withIdleConnectionTime(30.seconds).withTimeout(10.seconds).build
      ioToConnectionIO <- WeakAsync.liftK[IO, ConnectionIO]
    } yield Resources(transactor, channel, client, ioToConnectionIO)

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
}
