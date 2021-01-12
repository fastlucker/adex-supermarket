#!/usr/bin/env bash

# Run the benchmark using 
# t1 - one thread
# c100 - one hundred concurrent connections
# d30s - 30 seconds
# R2000 - 2000 requests per second (total, across all connections combined)
# R100000000 = 100_000_000
wrk2 -t$1 -c1000 -d30s -R100000000 --latency http://127.0.0.1:3000/$2
