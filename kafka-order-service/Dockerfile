FROM eclipse-temurin:21-jre-alpine
RUN mkdir -p /data
WORKDIR /app
COPY target/kafka-order-service-1.0.0-SNAPSHOT.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
