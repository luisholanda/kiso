#!/usr/bin/env bash
readonly ECHO_SERVER="$1"
readonly PROTOFILE="$2"

shift;
shift;

function benchmark_header() {
  echo "Benchmarking method: grpc.examples.echo.Echo/$1..."
}

function benchmark_ghz() {
  benchmark_header "$1"
  local method="grpc.examples.echo.Echo/$1"
  shift;
  ghz --insecure --async --cpus=1 --call="$method" --proto="$PROTOFILE" "$@" localhost:8080
}

$ECHO_SERVER > /dev/null 2>&1 &
readonly server_pid=$!

sleep 2

benchmark_ghz UnaryEcho \
  --total=21000 \
  --skipFirst=1000 \
  --rps=2000 \
  --connections=10 \
  --concurrency=100 \
  --data='{"message":"{{.Timestamp}}"}'

benchmark_ghz ServerStreamingEcho \
  --total=11000 \
  --skipFirst=1000 \
  --rps=1000 \
  --connections=10 \
  --concurrency=100 \
  --data='{"message":"{{.Timestamp}}"}'


kill "$server_pid"
