package schemakeeper.server.util

import com.twitter.finagle.http.filter.Cors
import schemakeeper.server.Configuration

object Filter {
  def corsFilterPolicy(configuration: Configuration): Cors.HttpFilter = {
    val policy: Cors.Policy = Cors.Policy(
      allowsOrigin = _ => configuration.allowsOrigin,
      allowsMethods = _ => configuration.allowsMethods,
      allowsHeaders = _ => configuration.allowsHeaders
    )

    new Cors.HttpFilter(policy)
  }
}
