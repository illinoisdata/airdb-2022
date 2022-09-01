sudo echo '3' > /proc/sys/vm/drop_caches
./bin/ycsb run airkv -s -P workloads/workloadc -p recordcount=10 -p operationcount=10 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p airkv.block.limit=2000 > ./airkv_log/sample_run &

