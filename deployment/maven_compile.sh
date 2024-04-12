#!/bin/bash


#apt-get update
#apt-get install -y mvn
cd .. && mvn clean package docker:build
