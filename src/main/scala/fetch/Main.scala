package fetch

import cats.implicits._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Main extends App {
  import Fetch._

  case class Avatar(body: String)
  case class Contact(name: String, avatar: Avatar)

  val names = List("Alice", "Bob", "Charlie")

  // Dummy response handler:
  def callback(response: String): Future[String] =
    Future.successful(response)

  val program = names.traverse { name =>
    val n = Fetch.pure(name)
    val a = Fetch.lookup("http://example.com/avatar/*", name)(callback).map(Avatar)
    (n, a).mapN(Contact)
  }

  val naive = new NaiveInterpreter()
  val optimized = new OptimizedInterpreter(naive)

  println("NAIVE")
  Await.result(naive(program), 1.second)

  println("OPTIMIZED")
  Await.result(optimized(program), 1.second)
}
