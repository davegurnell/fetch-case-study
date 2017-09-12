package fetch

import cats.{Applicative, ~>}

import scala.concurrent.{ExecutionContext => EC, _}

/**
  * An applicative style control structure for pulling data from remote web services.
  * The idea to optimise sequences of requests like:
  *
  * GET http://example.com/1
  * GET http://example.com/2
  * GET http://example.com/3
  *
  * into a single request:
  *
  * GET http://example.com/1,2,3
  *
  * We're just assuming we only access web services that permit this optimization.
  * A real system would have to distinguish between requests that can be merged and ones that cannot.
  *
  * @tparam A the type of value generated.
  */
sealed trait Fetch[A]

object Fetch {

  // Fetch instances ============================

  /**
    * Dummy Fetch instance that just generates a value.
    */
  final case class Local[A](value: Future[A]) extends Fetch[A]

  /**
    * Lookup a bunch of primary keys on a remote service and handle the response.
    * We ignore some details below like how to find parts of the response.
    */
  final case class Lookup[A](urlPattern: UrlPattern, primaryKeys: List[String], callback: Response => Future[A]) extends Fetch[A]

  /**
    * Fetch instance representing running a `left` instance then mapping the results through `func`.
    */
  final case class Map[A, B](left: Fetch[A], func: A => B) extends Fetch[B]

  /**
    * Fetch instance representing running `left` and `right` instances then combining the results.
    */
  final case class Ap[A, B](left: Fetch[A], right: Fetch[A => B]) extends Fetch[B]

  // Constructors ===============================

  def pure[A](value: => A): Fetch[A] =
    Local(Future.successful(value))

  def async[A](value: => Future[A]): Fetch[A] =
    Local(value)

  def lookup[A](urlPattern: UrlPattern, key: String)(callback: String => Future[A]): Fetch[A] =
    Lookup(urlPattern, List(key), callback)

  def lookupAll[A](urlPattern: UrlPattern, keys: List[String])(callback: String => Future[A]): Fetch[A] =
    Lookup(urlPattern, keys, callback)

  // Type class instances =======================

  /**
    * Cats applicative instance that provides all the relevant combinator functions for fetch.
    *
    * Note that I haven't made Fetch a monad.
    * There's nothing to stop this in principle,
    * as long as we manually implement `ap`
    * to override the default monad implementation in terms of `flatMap`.
    */
  implicit object applicative extends Applicative[Fetch] {
    override def pure[A](x: A): Fetch[A] =
      Local(Future.successful(x))

    override def ap[A, B](ff: Fetch[A => B])(fa: Fetch[A]): Fetch[B] =
      Ap(fa, ff)
  }
}

/**
  * A naive interpreter that interprets the fetch programme as-is.
  */
class NaiveInterpreter(implicit ec: EC) extends (Fetch ~> Future) {
  import Fetch._

  def apply[A](fetch: Fetch[A]): Future[A] = fetch match {
    case Local(value) =>
      value

    case Lookup(urlPattern, primaryKeys, callback) =>
      val url = urlPattern.replaceAll("[*]", primaryKeys.mkString(","))
      println("Performing LookupAll: " + url)
      callback(url)

    case Map(source, func) =>
      this.apply(source).map(func)

    case Ap(left, right) =>
      val leftFuture = this.apply(left)
      val rightFuture = this.apply(right)
      for {
        value <- leftFuture
        func <- rightFuture
      } yield func(value)
  }
}

/**
  * An interpreter that attempts to fuse Lookup steps
  * so we fetch multiple keys from the same URL pattern at the same time.
  * Examples:
  *
  * - it turns a map(lookup, func) into a straight lookup;
  * - it turns an ap(lookup, lookup) into a single lookup if both have the same URL pattern;
  * - and so on...
  *
  * It's a pretty crappy implementation.
  * It doesn't deal with a bunch of complex interleaved cases, for example:
  *
  *    ap(lookupFromPattern1, ap(lookupFromPattern2, ap(lookupFromPattern1, ...)))
  *
  * One would be better off simply interpreting in two traversals of the fetch structure:
  *
  * 1. traverse the entire Fetch program looking for URL patterns and keys;
  * 2. kick off some optimized web requests and build a cache of results;
  * 3. traverse the entire Fetch program again,
  *    passing the cache around and dishing responses out to the various steps.
  */
class OptimizedInterpreter(inner: Fetch ~> Future)(implicit ec: EC) extends (Fetch ~> Future) {
  import Fetch._

  def apply[A](fetch: Fetch[A]): Future[A] =
    inner(optimize(fetch))

  def optimize[A](fetch: Fetch[A]): Fetch[A] = fetch match {
    case Local(value) =>
      Local(value)

    case Lookup(urlPattern, primaryKeys, callback) =>
      Lookup(urlPattern, primaryKeys, callback)

    case Map(source, func) =>
      optimize(source) match {
        case Lookup(urlPattern, primaryKeys, callback) =>
          Lookup(urlPattern, primaryKeys, response => {
            for {
              value <- callback(response)
            } yield func(value)
          })

        case other =>
          Map(other, func)
      }

    case Ap(left, right) =>
      val l = optimize(left)
      val r = optimize(right)
      (l, r) match {
        case (Lookup(lPattern, lKeys, lCallback), Lookup(rPattern, rKeys, rCallback)) if lPattern == rPattern =>
          Lookup(lPattern, lKeys ++ rKeys, response => {
            val v = lCallback(response)
            val f = rCallback(response)
            for {
              v <- v
              f <- f
            } yield f(v)
          })

        case (Lookup(lPattern, lKeys, lCallback), other) =>
          Lookup(lPattern, lKeys, response => {
            val v = lCallback(response)
            val f = inner(other)
            for {
              v <- v
              f <- f
            } yield f(v)
          })

        case (other, Lookup(rPattern, rKeys, rCallback)) =>
          Lookup(rPattern, rKeys, response => {
            val v = inner(other)
            val f = rCallback(response)
            for {
              v <- v
              f <- f
            } yield f(v)
          })

        case (l, r) =>
          Ap(l, r)
      }
  }
}
