#!/bin/bash
# Configures and starts one node of the 3-FE test cluster, inside its container.
#   cluster3.sh leader      (starrocks-dev: FE + BE, data under /sr-data2)
#   cluster3.sh follower <leader ip>   (starrocks-fe2 / starrocks-fe3: FE only)
set -e
export JAVA_HOME=/lib/jvm/java-21-openjdk
IP=$(hostname -I | awk '{print $1}')
conf_fe() { # conf_fe <fe home> <heap>
  sed -i '/^# e2e/,$d' $1/conf/fe.conf
  sed -i "s/-Xmx[0-9]*m/-Xmx$2/" $1/conf/fe.conf
  cat >> $1/conf/fe.conf <<CONF
# e2e
priority_networks = $IP/32
meta_dir = /sr-data2/meta
sys_log_dir = /sr-data2/fe-log
audit_log_dir = /sr-data2/fe-log
CONF
  mkdir -p /sr-data2/meta /sr-data2/fe-log
}
wait_mysql() { for i in $(seq 1 150); do mysql -h$IP -P9030 -uroot -e "select 1" >/dev/null 2>&1 && return 0; sleep 2; done; return 1; }
if [ "$1" = leader ]; then
  FE=/sr-output/fe; BE=/sr-output/be
  conf_fe $FE 1536m
  sed -i '/^# e2e/,$d' $BE/conf/be.conf
  cat >> $BE/conf/be.conf <<CONF
# e2e
priority_networks = $IP/32
storage_root_path = /sr-data2/storage
sys_log_dir = /sr-data2/be-log
mem_limit = 2G
CONF
  mkdir -p /sr-data2/storage /sr-data2/be-log
  $FE/bin/start_fe.sh --daemon 2>/dev/null
  wait_mysql
  mysql -h$IP -P9030 -uroot -e "ALTER SYSTEM ADD BACKEND '$IP:9050'" 2>/dev/null || true
  $BE/bin/start_be.sh --daemon 2>/dev/null
  for i in $(seq 1 90); do
    alive=$(mysql -h$IP -P9030 -uroot -N -e "SHOW BACKENDS" 2>/dev/null | awk -F'\t' '{print $9}')
    [ "$alive" = "true" ] && echo "leader and backend up on $IP" && exit 0; sleep 2
  done
  echo "backend not alive"; exit 1
else
  LEADER=$2; FE=/opt/fe
  if [ ! -d $FE ]; then
    mkdir -p $FE
    cp -r /opt/fe-pkg/bin /opt/fe-pkg/conf /opt/fe-pkg/webroot $FE/
    for d in lib spark-dpp hive-udf; do ln -s /opt/fe-pkg/$d $FE/$d; done
    FIRST=1
  fi
  conf_fe $FE 1024m
  # The leader side (ALTER SYSTEM ADD FOLLOWER) is run from the leader's container.
  if [ -n "$FIRST" ]; then $FE/bin/start_fe.sh --helper $LEADER:9010 --daemon 2>/dev/null
  else $FE/bin/start_fe.sh --daemon 2>/dev/null; fi
  echo "follower started on $IP"
fi
