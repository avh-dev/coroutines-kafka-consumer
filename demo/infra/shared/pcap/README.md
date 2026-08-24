# Kafka pcap analysis

`analyze-pcap.py` reads one capture, a `diagnostics/tcpdump` directory, or a
complete run directory. It uses tshark for frames, TCP streams, TLS records,
Kafka APIs, and reassembled payloads. Because tshark does not decode every
modern Produce/Fetch version, the analyzer also locates Kafka RecordBatch v2
payloads and decompresses gzip, Snappy, LZ4, and Zstandard batches through the
system libraries installed with tshark.

```bash
python3 demo/infra/shared/pcap/analyze-pcap.py \
  /opt/ckc-lab/results/runs/<run-id>
```

The output directory contains `summary.json` for report generation and
`summary.txt` for inspection. The runtime writes `analyzer.log` alongside them.

The byte categories are exhaustive for the captured Kafka TCP streams:

- `network_header_bytes`: captured link-layer, IP, and TCP headers. Ethernet
  FCS and bytes omitted by capture truncation cannot be counted.
- `tcp_payload_bytes`: all TCP payload, including Kafka and TLS.
- `kafka_protocol_bytes_excluding_batches`: request/response framing and Kafka
  protocol data outside RecordBatch bodies.
- `batch_header_bytes`: the 61-byte RecordBatch v2 envelope for each batch.
- `compressed_record_bytes`: record data as carried on the wire.
- `uncompressed_record_bytes`: the same record data after decompression. The
  compression ratio is compressed divided by uncompressed size; 100% therefore
  means no size reduction, while `space_saving_percent` is the reduction.

Connection counts describe the capture window: `observed` includes every TCP
stream, `opened_during_capture` requires an observed initial SYN, and
`active_at_capture_start` covers streams whose SYN preceded the capture. TLS
captures retain network, connection, and TLS-record measurements, but Kafka API
and batch estimates are marked unavailable because payload decryption keys are
not collected.
