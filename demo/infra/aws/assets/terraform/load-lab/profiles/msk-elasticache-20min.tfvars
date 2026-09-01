kafka_mode       = "msk"
elasticache_mode = "elasticache"

node_instance_types = ["m7i.xlarge"]
node_desired_size   = 3
node_min_size       = 3
node_max_size       = 3

msk_number_of_broker_nodes = 3
msk_broker_instance_type   = "kafka.m5.large"
msk_ebs_volume_size        = 50

elasticache_node_type          = "cache.r7g.large"
elasticache_num_cache_clusters = 2
