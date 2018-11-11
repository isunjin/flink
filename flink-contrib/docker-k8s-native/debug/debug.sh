#!/usr/bin/env bash

#mount flink distribution root to minikube, this is blocking
minikube mount ./../../../build-target/:/flink-dist
minikube mount ~/git/isunjin/flink_1/:/flink-root/

#create pv to minikube
kubectl create -f pv.yml

#share local registry
eval $(minikube docker-env)
