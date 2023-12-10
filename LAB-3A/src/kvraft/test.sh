#!/bin/bash
for (( i = 0; i < 100; i++ )); do
    echo "-----------第 $(($i + 1)) 次循环------------------"
    go test -run 3A >> test3A.txt
    echo "---------------------------------"
done