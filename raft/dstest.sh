#!/usr/bin/zsh

cat test_test.go| grep 2B | sed 's\(\ \g'|awk '/func/ {printf "%s ",$2;}' | xargs dstest -o .run -n 1000 -p 50 -r