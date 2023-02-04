#!/bin/bash -eu
if [ $# -ne 1 ] ; then
   PROJECT_NAME=$(basename $(realpath .))
else
   PROJECT_NAME=$1
fi

find . -name .git -prune -o -exec sed -i.bk "s/EarningsToS3/${PROJECT_NAME}/g" {} \;
find . -name '*.bk' -exec rm {} \;
mv src/EarningsToS3.py src/${PROJECT_NAME}.py
git add .
git ci -am "Convert run_log to ${PROJECT_NAME}"
