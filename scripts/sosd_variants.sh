#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 6 ]
then
  echo "Require 6 argument (BLOB_ROOT, KEYSET_ROOT, DB_ROOT, ACTION, REPEAT, RESET_SCRIPT), $# provided"
  echo 'Example: bash scripts/sosd_experiment.sh file://$(pwd)/../SOSD/data file://$(pwd)/../SOSD/keyset file://$(pwd)/tmp/btree btree btree build 1 ~/reload_nfs.sh'
  echo 'Example: bash scripts/sosd_experiment.sh file://$(pwd)/../SOSD/data file://$(pwd)/../SOSD/keyset file://$(pwd)/tmp/enb_stb enb step,band_greedy,band_equal build 1 ~/reload_nfs.sh'
  exit 1
fi

BLOB_ROOT=$1
KEYSET_ROOT=$2
DB_ROOT=$3
ACTION=$4
REPEAT=$5
RESET_SCRIPT=$6
OUT_PATH="sosd_variants_out.jsons"
PROFILE="--affine-latency-ns 12000000 --affine-bandwidth-mbps 50.0"  # nfs
# PROFILE="--affine-latency-ns 900000 --affine-bandwidth-mbps 125.0"  # local
LOG_LEVEL="info"
# LOG_LEVEL="debug"
# LOG_LEVEL="trace"
echo "Using BLOB_ROOT=${BLOB_ROOT}, KEYSET_ROOT=${KEYSET_ROOT}, DB_ROOT=${DB_ROOT}, ACTION=${ACTION}, REPEAT=${REPEAT} RESET_SCRIPT=${RESET_SCRIPT}, PROFILE=${PROFILE}, LOG_LEVEL=${LOG_LEVEL}"
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
  # "books 800 uint64"
  "fb 200 uint64"
  # "lognormal 200 uint32"
  # "lognormal 200 uint64"
  # "normal 200 uint32"
  # "normal 200 uint64"
  # "osm_cellids 200 uint64"
  # "osm_cellids 400 uint64"
  # "osm_cellids 600 uint64"
  # "osm_cellids 800 uint64"
  # "uniform_dense 200 uint32"
  # "uniform_dense 200 uint64"
  # "uniform_sparse 200 uint32"
  # "uniform_sparse 200 uint64"
  # "wiki_ts 200 uint64"
  # "gmm_k100 800 uint64"
)

# SOSD_BLOBS=(
#   "fb 1 uint64"  # for debugging
# )


##################################################################
### Number of layers

LAYERS=(
  "1"
  "2"
  "3"
  "4"
  "5"
)

experiment_layers () {
  read -a sosd_blob <<< $1
  target_layers=$2
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"
  db_path="${DB_ROOT}/layer_${target_layers}/${blob_name}"

  if [[ $ACTION == "build" ]]
  then
    set -x
    RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url ${db_path} --index-builder enb_layers --target-layers ${target_layers} --index-drafters=step,band_greedy,band_equal --out-path sosd_build_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --no-cache --do-build
    set +x
  elif [[ $ACTION == "benchmark" ]]
  then
    for ((k = 0; k < ${REPEAT}; k++)) do
    bash ${RESET_SCRIPT}
    set -x
    RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url ${db_path} --index-builder enb_layers --target-layers ${target_layers} --index-drafters=step,band_greedy,band_equal --out-path ${OUT_PATH} --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks_${k}" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --do-benchmark
    set +x
    done
  fi
}

for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  for ((j = 0; j < ${#LAYERS[@]}; j++)) do
    echo "${SOSD_BLOBS[$i]} with ${LAYERS[$j]} layers"
    experiment_layers "${SOSD_BLOBS[$i]}" "${LAYERS[$j]}"
  done
done

##################################################################
### Set of models

DRAFTERS=(
  "step s"
  "band_greedy bg"
  "band_equal be"
  "step,band_greedy sbg"
  "step,band_equal sbe"
  "band_greedy,band_equal sbgbe"
)

experiment_drafters () {
  read -a sosd_blob <<< $1
  read -a drafters <<< $2
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"
  db_path="${DB_ROOT}/drafter_${drafters[1]}/${blob_name}"

  if [[ $ACTION == "build" ]]
  then
    set -x
    RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url ${db_path} --index-builder enb --index-drafters=${drafters[0]} --out-path sosd_build_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --no-cache --do-build
    set +x
  elif [[ $ACTION == "benchmark" ]]
  then
    for ((k = 0; k < ${REPEAT}; k++)) do
    bash ${RESET_SCRIPT}
    set -x
    RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url ${db_path} --index-builder enb --index-drafters=${drafters[0]} --out-path ${OUT_PATH} --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks_${k}" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --do-benchmark
    set +x
    done
  fi
}

for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  for ((j = 0; j < ${#DRAFTERS[@]}; j++)) do
    echo "${SOSD_BLOBS[$i]} with ${DRAFTERS[$j]}"
    experiment_drafters "${SOSD_BLOBS[$i]}" "${DRAFTERS[$j]}"
  done
done

##################################################################
### Fixed load hyperparameter

LOADS=(
  "256"
  "1024"
  "4096"
  "16384"
  "65536"
  "262144"
  "1048576"
)

experiment_load () {
  read -a sosd_blob <<< $1
  load=$2
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"
  db_path="${DB_ROOT}/load_${load}/${blob_name}"

  if [[ $ACTION == "build" ]]
  then
    set -x
    RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url ${db_path} --index-builder enb --index-drafters=step,band_greedy,band_equal --low-load ${load} --high-load ${load} --out-path sosd_build_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --no-cache --do-build
    set +x
  elif [[ $ACTION == "benchmark" ]]
  then
    for ((k = 0; k < ${REPEAT}; k++)) do
    bash ${RESET_SCRIPT}
    set -x
    RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url ${db_path} --index-builder enb --index-drafters=step,band_greedy,band_equal --low-load ${load} --high-load ${load} --out-path ${OUT_PATH} --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks_${k}" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --do-benchmark
    set +x
    done
  fi
}

for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  for ((j = 0; j < ${#LOADS[@]}; j++)) do
    echo "${SOSD_BLOBS[$i]} with load= ${LOADS[$j]}"
    experiment_load "${SOSD_BLOBS[$i]}" "${LOADS[$j]}"
  done
done

##################################################################
### Search resolution

RESOLUTIONS=(
  "1.189207115002721"
  "1.4142135623730951"
  "2"
  "4"
  "16"
)

experiment_resolution () {
  read -a sosd_blob <<< $1
  resolution=$2
  sosd_size=${sosd_blob[1]}
  sosd_dtype=${sosd_blob[2]}
  blob_name="${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}"
  keyset_path="${KEYSET_ROOT}/${sosd_blob[0]}_${sosd_blob[1]}M_${sosd_blob[2]}_ks"
  db_path="${DB_ROOT}/resolution_${resolution}/${blob_name}"

  if [[ $ACTION == "build" ]]
  then
    set -x
    RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url ${db_path} --index-builder enb --index-drafters=step,band_greedy,band_equal --step-load ${resolution} --out-path sosd_build_out.jsons --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --no-cache --do-build
    set +x
  elif [[ $ACTION == "benchmark" ]]
  then
    for ((k = 0; k < ${REPEAT}; k++)) do
    bash ${RESET_SCRIPT}
    set -x
    RUST_LOG=airindex=${LOG_LEVEL},sosd_experiment=${LOG_LEVEL} RUST_BACKTRACE=full target/release/sosd_experiment --db-url ${db_path} --index-builder enb --index-drafters=step,band_greedy,band_equal --step-load ${resolution} --out-path ${OUT_PATH} --dataset-name blob --sosd-blob-url "${BLOB_ROOT}/${blob_name}" --keyset-url "${KEYSET_ROOT}/${blob_name}_ks_${k}" --sosd-dtype ${sosd_dtype} --sosd-size ${sosd_size} ${PROFILE} --do-benchmark
    set +x
    done
  fi
}

for ((i = 0; i < ${#SOSD_BLOBS[@]}; i++)) do
  for ((j = 0; j < ${#RESOLUTIONS[@]}; j++)) do
    echo "${SOSD_BLOBS[$i]} with resolution= ${RESOLUTIONS[$j]}"
    experiment_resolution "${SOSD_BLOBS[$i]}" "${RESOLUTIONS[$j]}"
  done
done
