COMPACTOR_NUM=2
AIRKV_COMPACT_DIR="/home/hippo/CIDR2023/airindex/airkv/target/debug"
mkdir -p airkv_log

for ((count = 1; count <= ${COMPACTOR_NUM}; count+=1)) do
	${AIRKV_COMPACT_DIR}/compaction_client > airkv_log/compactor_log_${count} &
done

