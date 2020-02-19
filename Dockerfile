FROM openjdk:8-jdk-alpine
ADD . /home/
WORKDIR /home/
RUN chmod +x mvnw
RUN ./mvnw clean install
EXPOSE 8090
EXPOSE 8082
ENTRYPOINT ["sh", "mvnw", "exec:exec", "-Dapp-name=pong"]