package schemakeeper.server.storage.lock

import doobie._
import doobie.implicits._
import doobie.free.connection.ConnectionIO

class MariaDBStorageLock extends StorageLock[ConnectionIO] {
  override def lockForUpdate(): ConnectionIO[Unit] = sql"lock table lock_table write".update.run.map(_ => ())
}

object MariaDBStorageLock {
  def apply(): MariaDBStorageLock = new MariaDBStorageLock()
}
