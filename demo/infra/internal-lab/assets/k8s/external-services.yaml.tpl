apiVersion: v1
kind: Service
metadata:
  name: ckc-external-kafka
  namespace: ckc-perf
spec:
  ports:
    - name: kafka
      port: 9092
      targetPort: 9092
---
apiVersion: v1
kind: Endpoints
metadata:
  name: ckc-external-kafka
  namespace: ckc-perf
subsets:
  - addresses:
      - ip: __LAB_NODE_IP__
    ports:
      - name: kafka
        port: 9092
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-external-redis
  namespace: ckc-perf
spec:
  ports:
    - name: redis
      port: 6379
      targetPort: 6379
---
apiVersion: v1
kind: Endpoints
metadata:
  name: ckc-external-redis
  namespace: ckc-perf
subsets:
  - addresses:
      - ip: __LAB_NODE_IP__
    ports:
      - name: redis
        port: 6379
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-external-audit
  namespace: ckc-perf
spec:
  ports:
    - name: audit-tcp
      port: 5170
      targetPort: 5170
---
apiVersion: v1
kind: Endpoints
metadata:
  name: ckc-external-audit
  namespace: ckc-perf
subsets:
  - addresses:
      - ip: __LAB_NODE_IP__
    ports:
      - name: audit-tcp
        port: 5170
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-external-loki
  namespace: ckc-perf
spec:
  ports:
    - name: http
      port: 3100
      targetPort: 3100
---
apiVersion: v1
kind: Endpoints
metadata:
  name: ckc-external-loki
  namespace: ckc-perf
subsets:
  - addresses:
      - ip: __LAB_NODE_IP__
    ports:
      - name: http
        port: 3100
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-external-kafka-exporter
  namespace: ckc-perf
spec:
  ports:
    - name: metrics
      port: 9308
      targetPort: 9308
---
apiVersion: v1
kind: Endpoints
metadata:
  name: ckc-external-kafka-exporter
  namespace: ckc-perf
subsets:
  - addresses:
      - ip: __LAB_NODE_IP__
    ports:
      - name: metrics
        port: 9308
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-external-redpanda-admin
  namespace: ckc-perf
spec:
  ports:
    - name: admin
      port: 9644
      targetPort: 9644
---
apiVersion: v1
kind: Endpoints
metadata:
  name: ckc-external-redpanda-admin
  namespace: ckc-perf
subsets:
  - addresses:
      - ip: __LAB_NODE_IP__
    ports:
      - name: admin
        port: 9644
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-external-process-exporter
  namespace: ckc-perf
spec:
  ports:
    - name: metrics
      port: 9256
      targetPort: 9256
---
apiVersion: v1
kind: Endpoints
metadata:
  name: ckc-external-process-exporter
  namespace: ckc-perf
subsets:
  - addresses:
      - ip: __LAB_NODE_IP__
    ports:
      - name: metrics
        port: 9256
