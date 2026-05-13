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
      - ip: __LAB_HOST_IP__
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
      - ip: __LAB_HOST_IP__
    ports:
      - name: redis
        port: 6379
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
      - ip: __LAB_HOST_IP__
    ports:
      - name: metrics
        port: 9308
