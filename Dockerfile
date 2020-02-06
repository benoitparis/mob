FROM openjdk:8-jdk-alpine
ADD . /
EXPOSE 8090
EXPOSE 8082
RUN mvnw -Dapp-name="pong" clean install exec:exec