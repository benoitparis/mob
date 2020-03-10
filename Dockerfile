# TODO: https://medium.com/@nieldw/caching-maven-dependencies-in-a-docker-build-dca6ca7ad612

FROM openjdk:8-jdk-alpine
ADD . /home/
WORKDIR /home/
RUN chmod +x mvnw
RUN ./mvnw clean install
EXPOSE 8090
EXPOSE 8082
ENTRYPOINT ["sh", "mvnw", "exec:exec", "-Dapp-name=conversation,pong"]