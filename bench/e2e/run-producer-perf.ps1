# Laptop/compose wrapper for kafka-producer-perf-test.sh (apache/kafka:4.4.0).
$ErrorActionPreference = "Stop"
$Bootstrap = if ($env:BOOTSTRAP) { $env:BOOTSTRAP } else { "127.0.0.1:9092" }
$Topic = if ($env:TOPIC) { $env:TOPIC } else { "bench" }
$Records = if ($env:RECORDS) { $env:RECORDS } else { "20000" }
$RecordSize = if ($env:RECORD_SIZE) { $env:RECORD_SIZE } else { "1024" }
$Throughput = if ($env:THROUGHPUT) { $env:THROUGHPUT } else { "-1" }
$Out = if ($env:OUT) { $env:OUT } else { "bench/e2e/laptop-producer-perf.csv" }
$Image = if ($env:KAFKA_IMAGE) { $env:KAFKA_IMAGE } else { "apache/kafka:4.4.0" }
New-Item -ItemType Directory -Force -Path (Split-Path $Out) | Out-Null
$ts = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
@(
  "# label=laptop"
  "# tool=kafka-producer-perf-test.sh"
  "# image=$Image"
  "# bootstrap=$Bootstrap"
  "# topic=$Topic"
  "# records=$Records"
  "# record_size=$RecordSize"
  "# throughput=$Throughput"
  "# acks=all"
  "# hardware=$([System.Environment]::OSVersion.VersionString) $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)"
  "# timestamp=$ts"
  "records_sent,records_per_sec,avg_latency_ms,max_latency_ms"
) | Set-Content -Encoding utf8 $Out
docker run --rm apache/kafka:4.4.0 `
  /opt/kafka/bin/kafka-producer-perf-test.sh `
  --topic $Topic --num-records $Records --record-size $RecordSize --throughput $Throughput `
  --producer-props "bootstrap.servers=$Bootstrap" acks=all linger.ms=200 |
  Tee-Object -FilePath $Out -Append
