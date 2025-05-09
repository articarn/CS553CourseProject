#! /bin/bash
sudo apt update
sudo apt-get upgrade
sudo apt install default-jdk -y
sudo apt install curl scala -y
sudo apt-get install nfs-common
curl -O https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar -xzvf spark-3.5.5-bin-hadoop3.tgz
rm spark-3.5.5-bin-hadoop3.tgz
sudo mkdir -p /mount/isnfs/qfileshare # Name of NFS share created in Azure
sudo mount -t nfs isnfs.file.core.windows.net:/isnfs/qfileshare /mount/isnfs/qfileshare -o vers=4,minorversion=1,sec=sys,nconnect=4
sudo mkdir /mount/isnfs/qfileshare/spark-logs
printf "spark.eventLog.enabled true\nspark.eventLog.dir /mount/isnfs/qfileshare/spark-logs/\nspark.history.fs.logDirectory /mount/isnfs/qfileshare/spark-logs\nspark.driver.memory 5g" >> spark-3.5.5-bin-hadoop3/conf/spark-defaults.conf