#!/usr/bin/zsh

cat test_test.go| grep "Backup" | sed 's\(\ \g'|awk '/func/ {printf "%s ",$2;}' | xargs dstest -o .run -n 5000 -p 30 -r