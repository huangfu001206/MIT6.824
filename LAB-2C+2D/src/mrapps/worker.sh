#!/bin/bash
go build -buildmode=plugin wc.go
# shellcheck disable=SC2164
cd ../main
go run mrworker.go ../mrapps/wc.so
