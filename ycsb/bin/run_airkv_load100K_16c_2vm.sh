#sudo echo '3' > /proc/sys/vm/drop_caches
AIRKV_DIR="/home/hippo/CIDR2023/airindex/airkv/target/debug"

mkdir -p airkv_log

#${AIRKV_DIR}/meta_prepare | tee  airkv_log/meta_prepare_log
#echo "META_PREPARE SUCCESS!"

VM_ID=1
CLIENT_NUM_PER_VM=8
INDEX_START=$((50000*VM_ID))
LOG_START=$((VM_ID*CLIENT_NUM_PER_VM))

for ((count = 0; count < ${CLIENT_NUM_PER_VM}; count+=1)) do
    START=$((INDEX_START+6250*count))
    #echo ${START}
    ./bin/ycsb load airkv -s -P workloads/workloadc -p insertstart=${START} -p insertcount=6250 -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p recordcount=100000 -p airkv.block.limit=2000 > ./airkv_log/load_16c_$((LOG_START+count)) &
done
