#sudo echo '3' > /proc/sys/vm/drop_caches
#echo 3 | sudo -i tee /proc/sys/vm/drop_caches
CLIENT_NUM_PER_VM=8
VM_ID=1
LOG_INDEX_START=$((CLIENT_NUM_PER_VM*VM_ID))
for ((count = 0; count < ${CLIENT_NUM_PER_VM}; count+=1)) do
        ./bin/ycsb run airkv -s -P workloads/workloadc -p recordcount=100000 -p airkv.dir=az:///airkvycsb/ -p airkv.dbtype=AzureStore -p airkv.block.limit=2000 > ./airkv_log/run_8c_$((LOG_INDEX_START+count)) &
done
