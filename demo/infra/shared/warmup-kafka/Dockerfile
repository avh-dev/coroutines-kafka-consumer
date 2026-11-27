FROM docker.io/apache/kafka:4.3.1 AS kafka

FROM docker.io/eclipse-temurin:25-jdk AS prewarm-builder
COPY --from=kafka /opt/kafka/libs/ /opt/kafka/libs/
COPY src/ /src/
RUN mkdir -p /classes \
    && javac --release 17 -cp '/opt/kafka/libs/*' -d /classes /src/PrewarmBroker.java

FROM kafka

USER root
COPY scripts/ /opt/ckc-warmup-kafka/bin/
COPY cli/ /opt/ckc-warmup-kafka/cli/
COPY --from=prewarm-builder /classes/ /opt/ckc-warmup-kafka/classes/
RUN chmod 0755 /opt/ckc-warmup-kafka/bin/*.sh \
    && mkdir -p /var/run/ckc-warmup-kafka \
    && chown -R appuser:appuser /opt/ckc-warmup-kafka /var/run/ckc-warmup-kafka

USER appuser
ENTRYPOINT ["/opt/ckc-warmup-kafka/bin/run.sh"]
CMD ["/etc/kafka/docker/run"]
