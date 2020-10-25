#!/usr/bin/env bash

for i in {01..08}; do
    ./leader --addr ":90${i}" &
done
