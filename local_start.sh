#!/bin/bash

# usage: bash local_start.sh

prepare_gensort() {
	TARFILE=./data/gensort-linux-1.5.tar.gz
	tar xf $TARFILE -C ./data
}

run_gensort() {
	mkdir -p ./data/generated_data
	for i in {0..19}; do
		b=$((i*1000))
		./data/64/gensort -b$b 1000 ./data/generated_data/partitioned_data_$i
	done
}

url="http://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz"
path="./data/gensort-linux-1.5.tar.gz"
curl -o "$path" "$url"
prepare_gensort
run_gensort
