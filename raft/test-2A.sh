#!/usr/bin/zsh

cat test_test.go| grep "2A" | sed 's/(/ /g'|awk '/func/ {printf "%s ",$2;}' | xargs dstest -o .1224_2 -n 100 -p 50 -r