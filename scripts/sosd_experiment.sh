#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 5 ]
then
  echo "Require three argument (BLOB_ROOT, KEYSET_ROOT, DB_ROOT, ACTION, RESET_SCRIPT), $# provided"
  exit 1
fi

BLOB_ROOT=$1
KEYSET_ROOT=$2
DB_ROOT=$3
ACTION=$4
RESET_SCRIPT=$5
PROFILE="--affine-latency-ns 108000000 --affine-bandwidth-mbps 104.0"  # nfs
# PROFILE="--affine-latency-ns 22000 --affine-bandwidth-mbps 2500.0"  # ssd
echo "Using BLOB_ROOT=${BLOB_ROOT}, KEYSET_ROOT=${KEYSET_ROOT}, DB_ROOT=${DB_ROOT}, ACTION=${ACTION}, RESET_SCRIPT=${RESET_SCRIPT}"
if [[ $ACTION != "build" && $ACTION != "benchmark" ]]
then
  echo "Invalid ACTION [build | benchmark]"
  exit 1
fi
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

build () {
  read -a sosd_blob <<< $1
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"

  set -x
  RUST_LOG=info RUST_BACKTRACE=full target/release/sosd_experiment --root-path "./" --db-path "${DB_ROOT}/${blob_name}" --out-path sosd_build_out.jsons --dataset-name blob --sosd-blob-path "${BLOB_ROOT}/${blob_name}" --keyset-path "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --do-build
  set +x
}

benchmark () {
  read -a sosd_blob <<< $1
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"

  for ((j = 0; j < 10; j++)) do
  bash ${RESET_SCRIPT}
  set -x
  RUST_LOG=info RUST_BACKTRACE=full target/release/sosd_experiment --root-path "./" --db-path "${DB_ROOT}/${blob_name}" --out-path sosd_benchmark_out.jsons --dataset-name blob --sosd-blob-path "${BLOB_ROOT}/${blob_name}" --keyset-path "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --do-benchmark
  set +x
  done
}

mkdir -p ${KEYSET_ROOT}
for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  if [[ $ACTION == "build" ]]
  then
    build "${SOSD_BLOBS[$i]}"
  elif [[ $ACTION == "benchmark" ]]
  then
    benchmark "${SOSD_BLOBS[$i]}"
  fi
done