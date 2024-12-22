#!/usr/bin/zsh

cat test_test.go| grep "TestManyElections2A" | sed 's\(\ \g'|awk '/func/ {printf "%s ",$2;}' | xargs dstest -o .run -n 1000 -p 30 -r