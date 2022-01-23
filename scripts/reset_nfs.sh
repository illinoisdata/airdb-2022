export AZURE_STORAGE_ACCOUNT="<INSERT_STORAGE_ACCOUNT>"
export AZURE_STORAGE_KEY="<INSERT_STORAGE_KEY>"
mkdir -p ~/e2e-test-dataset
sudo umount ~/e2e-test-dataset
sudo sysctl vm.drop_caches=3
sudo mount -o sec=sys,vers=3,nolock,proto=tcp ${AZURE_STORAGE_ACCOUNT}.blob.core.windows.net:/${AZURE_STORAGE_ACCOUNT}/e2e-test-dataset ~/e2e-test-dataset
sudo chmod 777 ~/e2e-test-dataset
(TIMEFORMAT=%R; time echo `cat ~/e2e-test-dataset/cran.sample.5 | wc -l` > /dev/null)
mkdir -p ~/index-nfs
sudo umount ~/index-nfs
sudo sysctl vm.drop_caches=3
sudo mount -o sec=sys,vers=3,nolock,proto=tcp ${AZURE_STORAGE_ACCOUNT}.blob.core.windows.net:/${AZURE_STORAGE_ACCOUNT}/pwl-index-nfs-2 ~/index-nfs
sudo chmod 777 ~/index-nfs
(TIMEFORMAT=%R; time echo `cat ~/index-nfs/cran.sample.5 | wc -l` > /dev/null)