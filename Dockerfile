FROM openjdk:8-jdk-alpine
ADD mvnw pom.xml ./
ADD .mvn .mvn
RUN chmod +x mvnw
RUN ./mvnw dependency:go-offline -Dmaven.artifact.threads=12

ADD . .
RUN chmod +x mvnw
RUN ./mvnw clean install

EXPOSE 8090
EXPOSE 8082
ENTRYPOINT ["sh", "mvnw", "exec:exec", "-Dapp-name=conversation,pong"]
