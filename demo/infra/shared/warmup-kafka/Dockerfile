FROM docker.io/apache/kafka:4.3.1

USER root
COPY scripts/ /opt/ckc-warmup-kafka/bin/
RUN chmod 0755 /opt/ckc-warmup-kafka/bin/*.sh \
    && mkdir -p /var/run/ckc-warmup-kafka \
    && chown -R appuser:appuser /opt/ckc-warmup-kafka /var/run/ckc-warmup-kafka

USER appuser
ENTRYPOINT ["/opt/ckc-warmup-kafka/bin/run.sh"]
CMD ["/etc/kafka/docker/run"]
