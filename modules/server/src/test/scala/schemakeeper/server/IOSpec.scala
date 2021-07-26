package schemakeeper.server

import cats.effect.IO

trait IOSpec extends BaseSpec {
  type F[A] = IO[A]

  def runF[A](fa: => F[Unit]): Unit = fa.unsafeRunSync()
}
