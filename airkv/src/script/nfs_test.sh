export AZURE_ACCOUNTNAME=
export AZURE_ACCOUNTKEY=

#the nfs container must exist before running this script
NFS_FOLDER="/home/azureuser/nfs_test/airkv/airkv-container"
CONTAINER_NAME="test-container"

rm -rf ${NFS_FOLDER}
mkdir ${NFS_FOLDER}

sudo mount -o sync,sec=sys,vers=3,nolock,proto=tcp ${AZURE_ACCOUNTNAME}.blob.core.windows.net:/${AZURE_ACCOUNTNAME}/${CONTAINER_NAME} ${NFS_FOLDER}
sudo sysctl vm.drop_caches=3

../target/debug/nfs_test file://${NFS_FOLDER}/1 50 > nfs_50.log

sudo umount ${NFS_FOLDER}