#!/usr/bin/zsh

cat test_test.go| grep FailNoAgree | sed 's\(\ \g'|awk '/func/ {printf "%s ",$2;}' | xargs dstest -o .run -n 100 -p 40 -r