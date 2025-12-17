#!/bin/sh

if [ "$1" == "run" ];
then
	mkdir -p build && cd build && cmake .. && cd tests && cmake --build .. && ./monitor
elif [ "$1" == "clear" ];
then
	rm -rf ./build/
else
	echo "Usage:"
	echo "  sh build.sh run || sh build.sh clear"
fi
