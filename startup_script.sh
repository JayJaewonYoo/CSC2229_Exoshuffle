#!/bin/bash

prepare_gensort() {
	sudo apt-get update
	TARFILE=/local/gensort-linux-1.5.tar.gz
	tar xf $TARFILE -C /local
}

run_gensort() {
	/local/gensort -c -b$1 $2 /local/generated_data
}
# See: https://github.com/exoshuffle/cloudsort/blob/91775e10347da91c76d95a13bf6c189a31ea4bef/cloudsort/sort_utils.py#L87
# 	This link shows code on running gensort
# 	Something like gensort -c -b0 9999 /local/generated_data
# 	-b0 means offset 0 or start at record 0, b100 would start at record 100
#	9999 is the number of binary records generated, this number can be changed to whatever is required
#	/local/generated_data is the path to the file where the records will be written to
#	Link to direct gensort documentation: http://www.ordinal.com/gensort.html


install_ray() {
	pip install -U \"ray[default]\"
}

prepare_gensort
run_gensort
install_ray
