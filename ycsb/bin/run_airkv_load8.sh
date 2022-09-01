#sudo echo '3' > /proc/sys/vm/drop_caches
mkdir -p airkv_log
./ycsb load airkv -s -P workloads/workloadc -p insertstart=0 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///integration/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_1 &
./ycsb load airkv -s -P workloads/workloadc -p insertstart=12500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///integration/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_2 &
./ycsb load airkv -s -P workloads/workloadc -p insertstart=25000 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///integration/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_3 &
./ycsb load airkv -s -P workloads/workloadc -p insertstart=37500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///integration/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_4 &
./ycsb load airkv -s -P workloads/workloadc -p insertstart=50000 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///integration/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_5 &
./ycsb load airkv -s -P workloads/workloadc -p insertstart=62500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///integration/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_6 &
./ycsb load airkv -s -P workloads/workloadc -p insertstart=75000 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///integration/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_7 &
./ycsb load airkv -s -P workloads/workloadc -p insertstart=87500 -p insertcount=12500 -p recordcount=100000 -p airkv.dir=az:///integration/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_8c_8 &
