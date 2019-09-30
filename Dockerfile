FROM gradle:jdk8-alpine as builder
COPY --chown=gradle:gradle . /project
WORKDIR /project
RUN gradle :schemakeeper-server:clean :schemakeeper-server:jar

FROM openjdk:8-jre-alpine
COPY --from=builder /project/schemakeeper-server/build/libs/schemakeeper-server-0.1.jar /app/server.jar
WORKDIR /app
CMD java -jar server.jar
