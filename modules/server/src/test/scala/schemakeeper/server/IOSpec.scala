package schemakeeper.server

import cats.effect.IO
import org.scalatest.Assertion

trait IOSpec extends BaseSpec {
  type F[A] = IO[A]

  def runF[A](fa: => F[Assertion]): Assertion = fa.unsafeRunSync()
}
