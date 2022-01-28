#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 3 ]
then
  echo "Require three argument (BLOB_ROOT, KEYSET_ROOT, NUM_KEYSET), $# provided"
  exit 1
fi

BLOB_ROOT=$1
KEYSET_ROOT=$2
NUM_KEYSET=$3
echo "Using BLOB_ROOT=${BLOB_ROOT}, KEYSET_ROOT=${KEYSET_ROOT}, NUM_KEYSET=${NUM_KEYSET}"
sleep 5

SOSD_BLOBS=(
  # "books 200 uint32"
  # "books 200 uint64"
  # "books 400 uint64"
  # "books 600 uint64"
  "books 800 uint64"
  "fb 200 uint64"
  # "lognormal 200 uint32"
  "lognormal 200 uint64"
  # "normal 200 uint32"
  "normal 200 uint64"
  # "osm_cellids 200 uint64"
  # "osm_cellids 400 uint64"
  # "osm_cellids 600 uint64"
  "osm_cellids 800 uint64"
  # "uniform_dense 200 uint32"
  "uniform_dense 200 uint64"
  # "uniform_sparse 200 uint32"
  "uniform_sparse 200 uint64"
  "wiki_ts 200 uint64"
)

mkdir -p ${KEYSET_ROOT}
for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  read -a sosd_blob <<< "${SOSD_BLOBS[$i]}"
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_path="${BLOB_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"

  set -x
  ./target/release/sosd_keyset --sosd-dtype ${sosd_dtype} --sosd-blob-path ${blob_path} --sosd-size ${sosd_size} --keyset-path ${keyset_path} --num-keyset ${NUM_KEYSET}
  set +x
done