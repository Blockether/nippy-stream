#!/bin/bash

set -e

cd src/java

find . -name "*.java" -print | xargs javac
