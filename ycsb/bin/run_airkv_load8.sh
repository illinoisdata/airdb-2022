#sudo echo '3' > /proc/sys/vm/drop_caches
AIRKV_DIR="/home/hippo/CIDR2023/airindex/airkv/target/debug"

mkdir -p airkv_log

${AIRKV_DIR}/meta_prepare | tee  airkv_log/meta_prepare_log
echo "META_PREPARE SUCCESS!"

./bin/ycsb load airkv -s -P workloads/workloadc -p insertstart=0 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_1 &
./bin/ycsb load airkv -s -P workloads/workloadc -p insertstart=12500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_2 &
./bin/ycsb load airkv -s -P workloads/workloadc -p insertstart=25000 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_3 &
./bin/ycsb load airkv -s -P workloads/workloadc -p insertstart=37500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_4 &
./bin/ycsb load airkv -s -P workloads/workloadc -p insertstart=50000 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_5 &
./bin/ycsb load airkv -s -P workloads/workloadc -p insertstart=62500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_6 &
./bin/ycsb load airkv -s -P workloads/workloadc -p insertstart=75000 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_7 &
./bin/ycsb load airkv -s -P workloads/workloadc -p insertstart=87500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_8 &

