#!/bin/bash
# fe-restart.sh <fe home> [stop]: stops the FE of this container by its pid and starts it again.
export JAVA_HOME=/lib/jvm/java-21-openjdk
FE=$1
pid=$(cat $FE/bin/fe.pid 2>/dev/null)
if [ -n "$pid" ]; then
  kill $pid 2>/dev/null
  for i in $(seq 1 60); do
    st=$(ps -o stat= -p $pid 2>/dev/null)
    [ -z "$st" ] || [[ "$st" == Z* ]] && break; sleep 1
  done
  st=$(ps -o stat= -p $pid 2>/dev/null); [ -n "$st" ] && [[ "$st" != Z* ]] && kill -9 $pid
fi
[ "$2" = stop ] && { echo "stopped"; exit 0; }
$FE/bin/start_fe.sh --daemon 2>/dev/null
echo "started"
