package schemakeeper.server.storage.lock

import doobie._
import doobie.implicits._
import doobie.free.connection.ConnectionIO

class OracleStorageLock extends StorageLock[ConnectionIO] {
  override def lockForUpdate(): ConnectionIO[Unit] = sql"lock table lock_table in exclusive mode".update.run.map(_ => ()
  )
}

object OracleStorageLock {
  def apply(): OracleStorageLock = new OracleStorageLock()
}
