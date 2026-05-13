FROM eclipse-temurin:21-jre
WORKDIR /app
COPY build/libs/*-all.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
