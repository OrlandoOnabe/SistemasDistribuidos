FROM eclipse-temurin:21-jdk-jammy

WORKDIR /app

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir lib

RUN curl -fL https://repo.maven.apache.org/maven2/org/zeromq/jeromq/0.5.4/jeromq-0.5.4.jar \
    -o lib/jeromq.jar

RUN curl -fL https://repo.maven.apache.org/maven2/com/google/protobuf/protobuf-java/3.21.12/protobuf-java-3.21.12.jar \
    -o lib/protobuf-java.jar

RUN curl -fL https://repo.maven.apache.org/maven2/org/xerial/sqlite-jdbc/3.46.1.3/sqlite-jdbc-3.46.1.3.jar \
    -o lib/sqlite-jdbc.jar

RUN curl -fL https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar \
    -o lib/slf4j-api.jar

RUN curl -fL https://repo.maven.apache.org/maven2/org/slf4j/slf4j-simple/2.0.16/slf4j-simple-2.0.16.jar \
    -o lib/slf4j-simple.jar

COPY ./mensagem.proto .

RUN protoc --java_out=. mensagem.proto

COPY ./java/Cliente.java .
COPY ./java/Servidor.java .

RUN javac -cp "lib/*" -d . Mensagem.java Cliente.java Servidor.java

CMD ["java", "-cp", ".:lib/*", "Cliente"]