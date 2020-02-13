package schemakeeper.server.storage.lock

import doobie.free.connection
import doobie.free.connection.ConnectionIO

class H2StorageLock extends StorageLock[ConnectionIO] {
  override def lockForUpdate(): ConnectionIO[Unit] = connection.pure(())

  override def unlock(): ConnectionIO[Unit] = connection.pure(())
}

object H2StorageLock {
  def apply(): H2StorageLock = new H2StorageLock()
}
