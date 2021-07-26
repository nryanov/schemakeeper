package schemakeeper.server.storage.lock

import doobie._
import doobie.implicits._
import doobie.free.connection.ConnectionIO

class MariaDBStorageLock extends StorageLock[ConnectionIO] {
  override def lockForUpdate(): ConnectionIO[Unit] =
    sql"select get_lock('schema_update_lock', 180)".query.unique.map(_ => ())

  override def unlock(): ConnectionIO[Unit] = sql"select release_lock('schema_update_lock')".query.unique.map(_ => ())
}

object MariaDBStorageLock {
  def apply(): MariaDBStorageLock = new MariaDBStorageLock()
}
