#!/bin/bash

# Start all streaming jobs in the background
spark-submit bloom_filter.py &
spark-submit cms_stream.py &
spark-submit edw_stream.py &
spark-submit flajolent_stream.py &
spark-submit lsh_before_diffrential_Privacy.py &
spark-submit lsh_stream.py &

# Wait for all background jobs to complete
wait 