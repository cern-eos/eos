#!/bin/sh
mkdir build && cd build && cmake .. && cd tests && cmake --build .. && ./monitor
