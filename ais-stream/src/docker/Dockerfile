FROM openjdk:13-oracle
VOLUME /tmp
ARG JAR_FILE
COPY ${JAR_FILE} app.jar
EXPOSE 9001
ENTRYPOINT ["java", "-jar", "/app.jar"]