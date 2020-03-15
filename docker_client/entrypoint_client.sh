#!/bin/bash
# TCSS558 Generic Client container - runs container continually 
# Exits container when /stop file is created by external process
echo "TCSS558 Generic Client container up...  "
while :
do
  if [ -f "/stop.txt" ]
  then
    exit
  fi
  sleep 1
done

