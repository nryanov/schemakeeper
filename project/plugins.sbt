resolvers += Resolver.bintrayRepo("alpeb", "sbt-plugins")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.3")
addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.7")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.8.2")

addSbtPlugin("com.cavorite" % "sbt-avro" % "3.1.0")
addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % "21.6.0")
addSbtPlugin("com.github.sbt" % "sbt-protobuf" % "0.7.0")

libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.9.0"
