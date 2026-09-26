/PartitionCount:/ {
  for (field_number = 1; field_number <= NF; field_number++) {
    if ($field_number == "PartitionCount:") partitions = $(field_number + 1)
    if ($field_number == "ReplicationFactor:") replication = $(field_number + 1)
  }
}

/Partition:/ && /Leader:/ {
  partition_rows++
  for (field_number = 1; field_number <= NF; field_number++) {
    if ($field_number == "Leader:" && $(field_number + 1) == "-1") bad = 1
    if ($field_number == "Isr:") {
      isr = $(field_number + 1)
      isr_count = split(isr, members, ",")
      if (isr_count != expected_replication) bad = 1
    }
  }
}

END {
  exit !(partitions == expected_partitions && replication == expected_replication && partition_rows == expected_partitions && !bad)
}
