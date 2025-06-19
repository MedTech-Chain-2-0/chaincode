FROM gradle:8.6-jdk11 AS builder

COPY --chown=gradle:gradle . /home/gradle/src

WORKDIR /home/gradle/src

RUN gradle --no-daemon shadowJar -x checkstyleMain -x checkstyleTest --parallel

# ---------- Stage: build OpenFHE (shared libs) ----------
FROM ubuntu:22.04 AS openfhe-build

ARG OPENFHE_REPO=https://github.com/openfheorg/openfhe-development.git
ARG OPENFHE_BRANCH=main

RUN apt-get update && apt-get install -y \
        build-essential \
        cmake \
        git \
        libomp-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt

# shallow clone to save time
RUN git clone --depth 1 --branch ${OPENFHE_BRANCH} ${OPENFHE_REPO} openfhe

WORKDIR /opt/openfhe

RUN mkdir build && cd build && \
    cmake .. -DBUILD_SHARED=ON -DBUILD_STATIC=OFF -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARKS=OFF -DBUILD_UNITTESTS=OFF && \
    make -j$(nproc) && \
    make install

# ---------- Stage: build bfv_calc binary ----------
FROM ubuntu:22.04 AS bfv-build

RUN apt-get update && apt-get install -y \
        build-essential \
        cmake \
        libomp-dev \
    && rm -rf /var/lib/apt/lists/*

# bring in OpenFHE install
COPY --from=openfhe-build /usr/local /usr/local

# copy app sources (produced by copy-fhe.sh): only src & CMakeLists
COPY fhe-src/CMakeLists.txt /fhe/
COPY fhe-src/src /fhe/src

WORKDIR /fhe

RUN mkdir build && cd build && \
    cmake .. -DUSE_SYSTEM_OPENFHE=ON && \
    make -j$(nproc) bfv_calc

# ---------- Final runtime stage ----------
FROM eclipse-temurin:11-jdk-jammy

WORKDIR /app

RUN apt-get update && apt-get install -y wget tar libgomp1 libomp5 && rm -rf /var/lib/apt/lists/*

# OR-tools download (unchanged)
RUN wget https://github.com/google/or-tools/releases/download/v9.9/or-tools_amd64_ubuntu-22.04_java_v9.9.3963.tar.gz && \
    mkdir -p /usr/local/or-tools && \
    tar -xzf or-tools_amd64_ubuntu-22.04_java_v9.9.3963.tar.gz -C /usr/local/or-tools --strip-components=1 && \
    rm or-tools_amd64_ubuntu-22.04_java_v9.9.3963.tar.gz

RUN addgroup --system javauser && useradd -g javauser javauser

# make /app owned by javauser so chaincode can create fhe_data
RUN chown -R javauser:javauser /app

# chaincode jar & entrypoint
COPY --from=builder --chown=javauser:javauser /home/gradle/src/build/libs/medtechchain.jar /app/chaincode.jar
COPY --from=builder --chown=javauser:javauser /home/gradle/src/docker-entrypoint.sh /app/docker-entrypoint.sh

# OpenFHE libs and bfv_calc
COPY --from=openfhe-build /usr/local/lib/libOPENFHE* /usr/local/lib/
COPY --from=bfv-build /fhe/build/bfv_calc /app/bfv_calc

RUN chmod +x /app/bfv_calc /app/docker-entrypoint.sh

ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
ENV PORT 9999
EXPOSE 9999

USER javauser
ENTRYPOINT ["/app/docker-entrypoint.sh"]