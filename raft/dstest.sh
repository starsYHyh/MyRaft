#!/usr/bin/zsh

cat test_test.go| grep "2B" | sed 's\(\ \g'| awk '/func/ {printf "%s ",$2;}' | xargs dstest -o .1224_2 -n 5000 -p 100 -r