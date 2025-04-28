#!/usr/bin/zsh
# Install2D
# InstallUnreliable2D
# InstallCrash2D
# InstallUnCrash2D
# AllCrash2D
cat test_test.go| grep 2 | sed 's\(\ \g'| awk '/func/ {printf "%s ",$2;}' | xargs dstest -o .0209_01 -n 5 -p 10 -r
