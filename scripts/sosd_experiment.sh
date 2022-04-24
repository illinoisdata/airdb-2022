#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 8 ]
then
  echo "Require 8 argument (BLOB_ROOT, KEYSET_ROOT, DB_ROOT, INDEX_BUILDER, INDEX_DRAFTERS, ACTION, REPEAT, RESET_SCRIPT), $# provided"
  echo 'Example: bash scripts/sosd_experiment.sh file://$(pwd)/../SOSD/data file://$(pwd)/../SOSD/keyset file://$(pwd)/tmp/btree btree btree build 1 ~/reload_nfs.sh'
  echo 'Example: bash scripts/sosd_experiment.sh file://$(pwd)/../SOSD/data file://$(pwd)/../SOSD/keyset file://$(pwd)/tmp/enb_stb enb step,band_greedy,band_equal build 1 ~/reload_nfs.sh'
  exit 1
fi

BLOB_ROOT=$1
KEYSET_ROOT=$2
DB_ROOT=$3
INDEX_BUILDER=$4
INDEX_DRAFTERS=$5
ACTION=$6
REPEAT=$7
RESET_SCRIPT=$8
PROFILE="--affine-latency-ns 12000000 --affine-bandwidth-mbps 50.0"  # nfs
# PROFILE="--affine-latency-ns 900000 --affine-bandwidth-mbps 125.0"  # local
LOG_LEVEL="info"
# LOG_LEVEL="debug"
# LOG_LEVEL="trace"
echo "Using BLOB_ROOT=${BLOB_ROOT}, KEYSET_ROOT=${KEYSET_ROOT}, DB_ROOT=${DB_ROOT}, INDEX_BUILDER=${INDEX_BUILDER}, INDEX_DRAFTERS=${INDEX_DRAFTERS}, ACTION=${ACTION}, REPEAT=${REPEAT} RESET_SCRIPT=${RESET_SCRIPT}, PROFILE=${PROFILE}, LOG_LEVEL=${LOG_LEVEL}"
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
  # "lognormal 200 uint64"
  # "normal 200 uint32"
  # "normal 200 uint64"
  # "osm_cellids 200 uint64"
  # "osm_cellids 400 uint64"
  # "osm_cellids 600 uint64"
  "osm_cellids 800 uint64"
  # "uniform_dense 200 uint32"
  # "uniform_dense 200 uint64"
  # "uniform_sparse 200 uint32"
  # "uniform_sparse 200 uint64"
  "wiki_ts 200 uint64"
  "gmm_k100 800 uint64"
)

# SOSD_BLOBS=(
#   "fb 1 uint64"  # for debugging
# )

build () {
  read -a sosd_blob <<< $1
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"

  set -x
  RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url "${DB_ROOT}/${blob_name}" --index-builder ${INDEX_BUILDER} --index-drafters=${INDEX_DRAFTERS} --out-path sosd_build_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --no-cache --do-build
  set +x
}

benchmark () {
  read -a sosd_blob <<< $1
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"

  for ((j = 0; j < ${REPEAT}; j++)) do
  bash ${RESET_SCRIPT}
  set -x
  RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url "${DB_ROOT}/${blob_name}" --index-builder ${INDEX_BUILDER} --index-drafters=${INDEX_DRAFTERS} --out-path sosd_benchmark_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks_${j}" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --do-benchmark
  set +x
  done
}

for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  if [[ $ACTION == "build" ]]
  then
    build "${SOSD_BLOBS[$i]}"
  elif [[ $ACTION == "benchmark" ]]
  then
    benchmark "${SOSD_BLOBS[$i]}"
  fi
done