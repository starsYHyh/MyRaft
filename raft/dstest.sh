#!/usr/bin/zsh

cat test_test.go| grep "2B" | sed 's\(\ \g'|awk '/func/ {printf "%s ",$2;}' | xargs dstest -o .1223 -n 100 -p 40 -r