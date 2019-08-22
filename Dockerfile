FROM maven:3-jdk-8-slim

COPY . .

RUN mvn test
