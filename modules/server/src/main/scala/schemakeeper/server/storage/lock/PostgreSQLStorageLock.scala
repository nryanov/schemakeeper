package schemakeeper.server.storage.lock

import doobie._
import doobie.free.connection
import doobie.implicits._
import doobie.free.connection.ConnectionIO

class PostgreSQLStorageLock() extends StorageLock[ConnectionIO] {
  override def lockForUpdate(): ConnectionIO[Unit] = for {
    _ <- sql"lock table subject in share update exclusive mode".update.run
    _ <- sql"lock table schema_info in share update exclusive mode".update.run
    _ <- sql"lock table subject_schema in share update exclusive mode".update.run
  } yield ()

  override def unlock(): ConnectionIO[Unit] = connection.pure(())
}

object PostgreSQLStorageLock {
  def apply(): PostgreSQLStorageLock = new PostgreSQLStorageLock()
}
