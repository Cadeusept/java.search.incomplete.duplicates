#FROM openjdk:latest as build
FROM maven:3-eclipse-temurin-21-alpine AS maven
WORKDIR /workspace/app

#COPY mvnw .
#RUN chmod u+rxw ./mvnw
#COPY .mvn .mvn
#RUN apt-get update
#RUN apt-get install openjdk-21-jdk -y
COPY ../pom.xml .
COPY ../src src

RUN mvn compile # package
#RUN mkdir -p target/dependency && (cd target/dependency; jar -xf ../*.jar)

#FROM openjdk:latest
#ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/key.json
#ARG DEPENDENCY=/workspace/app/target/dependency
#COPY --from=build ${DEPENDENCY}/BOOT-INF/lib /app/lib
#COPY --from=build ${DEPENDENCY}/META-INF /app/META-INF
#COPY --from=build ${DEPENDENCY}/BOOT-INF/classes /app

ENTRYPOINT ["java", "-jar", "target/website-parser-1.0-SNAPSHOT.jar"]