#!/bin/sh
# 컴파일: out/ 에 class 파일 생성
cd "$(dirname "$0")"
mkdir -p out
javac -encoding UTF-8 -d out src/*.java
