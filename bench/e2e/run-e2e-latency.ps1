$ErrorActionPreference = "Stop"
$Root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
if (-not (Test-Path (Join-Path $Root "mvnw.cmd"))) { $Root = (Get-Location).Path }
$Bootstrap = if ($env:BOOTSTRAP) { $env:BOOTSTRAP } else { "127.0.0.1:9092" }
$Topic = if ($env:TOPIC) { $env:TOPIC } else { "bench-e2e" }
$Records = if ($env:RECORDS) { $env:RECORDS } else { "2000" }
$Out = if ($env:OUT) { $env:OUT } else { (Join-Path $Root "bench\e2e\laptop-e2e-latency.csv") }
Set-Location $Root
.\mvnw.cmd -q -pl bench -am exec:java "-Dexec.mainClass=lakewire.bench.e2e.E2eLatencyProbe" "-Dexec.args=--bootstrap $Bootstrap --topic $Topic --records $Records --output $Out"
