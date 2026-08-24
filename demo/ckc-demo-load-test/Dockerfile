FROM eclipse-temurin:21-jre
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tcpdump \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY build/install/ckc-demo-load-test/ /app/
ENTRYPOINT ["/app/bin/ckc-demo-load-test"]
