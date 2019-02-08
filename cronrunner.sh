#!/bin/bash
cd "$(dirname "$0")"

echo "process measurements"
node msm-prb-min-rtt.js

rm ./result_data/last/*
mv ./result_data/new/*.csv ./result_data/last/

ssh -ljasper -p16574 46.4.37.51 "rm /tmp/hmm/data/new/*.csv"
scp -o port=16574 ./result_data/*.csv jasper@46.4.37.51:/tmp/hmm/data/new/
