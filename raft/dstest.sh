#!/usr/bin/zsh

cat test_test.go| grep "2B" | sed 's\(\ \g'| awk '/func/ {printf "%s ",$2;}' | xargs dstest -o .run -n 100 -p 10 -r