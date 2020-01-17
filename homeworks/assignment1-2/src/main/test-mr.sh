#!/bin/bash
here=$(dirname "$0")
[[ "$here" = /* ]] || here="$PWD/$here"
export GOPATH="$here/../../"
echo ""
echo "==> Part A"
go test -run TestSequentialSingle mr/mapreduce/...
go test -run TestSequentialMany mr/mapreduce/...
echo ""
echo "==> Part B"
(cd "$here" && ./test-wc.sh > /dev/null)
echo ""
echo "==> Part C"
go test -run TestBasic mr/mapreduce/...
echo ""
echo "==> Part D"
go test -run TestOneFailure mr/mapreduce/...
go test -run TestManyFailures mr/mapreduce/...
echo ""
echo "==> Part E"
(cd "$here" && ./test-ii.sh > /dev/null)

rm "$here"/mrtmp.* "$here"/diff.out
