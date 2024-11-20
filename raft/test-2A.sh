#!/usr/bin/zsh

for i in {1..10} 
    go test -run 2A | dslogs -i INFO