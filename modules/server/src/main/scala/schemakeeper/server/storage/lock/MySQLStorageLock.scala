package schemakeeper.server.storage.lock

import doobie._
import doobie.implicits._
import doobie.free.connection.ConnectionIO

class MySQLStorageLock extends StorageLock[ConnectionIO] {
  override def lockForUpdate(): ConnectionIO[Unit] =
    sql"select get_lock('schema_update_lock', 180)".query.unique.map(_ => ())

  override def unlock(): ConnectionIO[Unit] = sql"select release_lock('schema_update_lock')".query.unique.map(_ => ())
}

object MySQLStorageLock {
  def apply(): MySQLStorageLock = new MySQLStorageLock()
}
