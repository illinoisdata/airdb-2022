#sudo echo '3' > /proc/sys/vm/drop_caches
#echo 3 | sudo -i tee /proc/sys/vm/drop_caches
./bin/ycsb run airkv -s -P workloads/workloadc -p insertstart=0 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/run_8c_1 &
./bin/ycsb run airkv -s -P workloads/workloadc -p insertstart=12500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/run_8c_2 &
./bin/ycsb run airkv -s -P workloads/workloadc -p insertstart=25000 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/run_8c_3 &
./bin/ycsb run airkv -s -P workloads/workloadc -p insertstart=37500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/run_8c_4 &
./bin/ycsb run airkv -s -P workloads/workloadc -p insertstart=50000 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/run_8c_5 &
./bin/ycsb run airkv -s -P workloads/workloadc -p insertstart=62500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/run_8c_6 &
./bin/ycsb run airkv -s -P workloads/workloadc -p insertstart=75000 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/run_8c_7 &
./bin/ycsb run airkv -s -P workloads/workloadc -p insertstart=87500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/run_8c_8 &

