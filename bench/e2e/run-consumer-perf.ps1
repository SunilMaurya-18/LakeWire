$ErrorActionPreference = "Stop"
$Bootstrap = if ($env:BOOTSTRAP) { $env:BOOTSTRAP } else { "127.0.0.1:9092" }
$Topic = if ($env:TOPIC) { $env:TOPIC } else { "bench" }
$Messages = if ($env:MESSAGES) { $env:MESSAGES } else { "20000" }
$Out = if ($env:OUT) { $env:OUT } else { "bench/e2e/laptop-consumer-perf.csv" }
$Image = if ($env:KAFKA_IMAGE) { $env:KAFKA_IMAGE } else { "apache/kafka:4.4.0" }
New-Item -ItemType Directory -Force -Path (Split-Path $Out) | Out-Null
$ts = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
@(
  "# label=laptop"
  "# tool=kafka-consumer-perf-test.sh"
  "# image=$Image"
  "# bootstrap=$Bootstrap"
  "# topic=$Topic"
  "# messages=$Messages"
  "# hardware=$([System.Environment]::OSVersion.VersionString) $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)"
  "# timestamp=$ts"
  "mb_sec,records_per_sec"
) | Set-Content -Encoding utf8 $Out
docker run --rm $Image `
  /opt/kafka/bin/kafka-consumer-perf-test.sh `
  --bootstrap-server $Bootstrap --topic $Topic --messages $Messages --timeout 60000 |
  Tee-Object -FilePath $Out -Append
