FROM openjdk:latest as build
WORKDIR /workspace/app

#COPY mvnw .
#RUN chmod u+rxw ./mvnw
#COPY .mvn .mvn
COPY pom.xml .
COPY src src

#RUN ./mvnw install -DskipTests
#RUN mkdir -p target/dependency && (cd target/dependency; jar -xf ../*.jar)
#
#FROM openjdk:latest
#ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/key.json
#ARG DEPENDENCY=/workspace/app/target/dependency
#COPY --from=build ${DEPENDENCY}/BOOT-INF/lib /app/lib
#COPY --from=build ${DEPENDENCY}/META-INF /app/META-INF
#COPY --from=build ${DEPENDENCY}/BOOT-INF/classes /app

ENTRYPOINT["java", "cp", "app:app/lib/*", "com.kajol.mywebapp.mywebappApplication"]