package schemakeeper.server.storage.lock

import doobie._
import doobie.implicits._
import doobie.free.connection.ConnectionIO

class PostgreSQLStorageLock() extends StorageLock[ConnectionIO] {
  override def lockForUpdate(): ConnectionIO[Unit] = sql"lock table lock_table access exclusive".update.run.map(_ => ())
}

object PostgreSQLStorageLock {
  def apply(): PostgreSQLStorageLock = new PostgreSQLStorageLock()
}
