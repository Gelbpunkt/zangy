#!/bin/bash
echo "aioredis python3.8 one-SET-at-once loop"
time python3.8 bench_aioredis_single.py
echo "zangy python 3.8 one-SET-at-once loop"
time python3.8 bench_zangy_single.py
echo "aioredis python3.8 1,000,000 SETs in parallel"
time python3.8 bench_aioredis.py
echo "zangy python3.8 1,000,000 SETs in parallel"
time python3.8 bench_zangy.py
