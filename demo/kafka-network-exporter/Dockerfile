FROM python:3.12-alpine

RUN apk add --no-cache nftables
COPY exporter.py /app/exporter.py
EXPOSE 9406
ENTRYPOINT ["python3", "/app/exporter.py"]
