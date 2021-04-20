#!/bin/bash
echo "aioredis python3 one-GET-at-once loop"
time python3 bench_aioredis_get.py
echo "zangy python3 one-GET-at-once loop"
time python3 bench_zangy_get.py
echo "aioredis python3 one-SET-at-once loop"
time python3 bench_aioredis_single.py
echo "zangy python3 one-SET-at-once loop"
time python3 bench_zangy_single.py
echo "aioredis python3 1,000,000 SETs in parallel"
time python3 bench_aioredis.py
echo "zangy python3 1,000,000 SETs in parallel"
time python3 bench_zangy.py
