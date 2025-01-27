#!/usr/bin/zsh

cat test_test.go| grep "TestFigure8Unreliable2C" | sed 's\(\ \g'|awk '/func/ {printf "%s ",$2;}' | xargs dstest -o .0127_1 -n 20 -p 10 -r