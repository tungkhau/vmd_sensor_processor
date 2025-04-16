FROM eclipse-temurin:22-jdk
WORKDIR /app
COPY target/vmd_sensor_processor-1.0.0-jar-with-dependencies.jar app.jar
CMD ["java", "-jar", "app.jar"]
