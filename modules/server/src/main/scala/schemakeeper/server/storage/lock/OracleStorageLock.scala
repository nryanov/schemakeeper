package schemakeeper.server.storage.lock

import doobie._
import doobie.free.connection
import doobie.implicits._
import doobie.free.connection.ConnectionIO

class OracleStorageLock extends StorageLock[ConnectionIO] {
  override def lockForUpdate(): ConnectionIO[Unit] =
    for {
      _ <- sql"lock table subject in share mode".update.run
      _ <- sql"lock table schema_info in share mode".update.run
      _ <- sql"lock table subject_schema in share mode".update.run
    } yield ()

  override def unlock(): ConnectionIO[Unit] = connection.pure(())
}

object OracleStorageLock {
  def apply(): OracleStorageLock = new OracleStorageLock()
}
