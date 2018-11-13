#!/usr/bin/env bash

./build.sh --from-local-dist --image-name flink-k8s

docker login --username=isunjin registry.cn-hangzhou.aliyuncs.com

docker tag flink-k8s registry.cn-hangzhou.aliyuncs.com/maxflink/flink:latest

docker push registry.cn-hangzhou.aliyuncs.com/maxflink/flink
