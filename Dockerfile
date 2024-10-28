from eclipse-temurin:22-jdk-alpine
copy ./pulse-receiver-service-0.0.1.jar app.jar
entrypoint ["java","-jar","app.jar"]