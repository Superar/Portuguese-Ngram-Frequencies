#!/bin/bash
for n in {1..5}
do
    uv run scripts/compute_ngrams.py -n $n
done
