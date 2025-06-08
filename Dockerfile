FROM gradle:8.6-jdk11 AS builder

COPY --chown=gradle:gradle . /home/gradle/src

WORKDIR /home/gradle/src

RUN gradle --no-daemon shadowJar -x checkstyleMain -x checkstyleTest --parallel

FROM eclipse-temurin:11-jdk-jammy

WORKDIR /app

RUN apt-get update && \
    apt-get install -y wget tar

RUN wget https://github.com/google/or-tools/releases/download/v9.9/or-tools_amd64_ubuntu-22.04_java_v9.9.3963.tar.gz && \
    mkdir -p /usr/local/or-tools && \
    tar -xzf or-tools_amd64_ubuntu-22.04_java_v9.9.3963.tar.gz -C /usr/local/or-tools --strip-components=1 && \
    rm or-tools_amd64_ubuntu-22.04_java_v9.9.3963.tar.gz

RUN addgroup --system javauser && useradd -g javauser javauser

COPY --from=builder --chown=javauser:javauser /home/gradle/src/build/libs/medtechchain.jar /app/chaincode.jar
COPY --from=builder --chown=javauser:javauser /home/gradle/src/docker-entrypoint.sh /app/docker-entrypoint.sh

RUN chmod +x /app/docker-entrypoint.sh

ENV PORT 9999
EXPOSE 9999

USER javauser
ENTRYPOINT [ "/app/docker-entrypoint.sh" ]