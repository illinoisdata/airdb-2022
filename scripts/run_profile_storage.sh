#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ -z "$1" ]
  then
    echo "Require one argument (ROOT, directory to profile on)"
    exit 1
fi

ROOT=$1
echo "Using ROOT=$ROOT"
sleep 5

READ_SIZES=(
  # Bs
  "1"
  "2"
  "4"
  "8"
  "16"
  "32"
  "64"
  "128"
  "256"
  "512"

  # KBs
  "1024"
  "2048"
  "4096"
  "8192"
  "16384"
  "32768"
  "65536"
  "131072"
  "262144"
  "524288"

  # MBs
  "1048576"
  "2097152"
  "4194304"
  "8388608"
  "16777216"
  "33554432"
  "67108864"
  "134217728"
  "268435456"
  "536870912"

  # # GBs
  # "1073741824"
  # "2147483648"
  # "4294967296"
  # "8589934592"
  # "17179869184"
  # "34359738368"
  # "68719476736"
  # "137438953472"
  # "274877906944"
  # "549755813888"
)

mkdir ${ROOT}/tmp_dir
for ((i = 0; i < ${#READ_SIZES[@]}; i++)) do
  read_size="${READ_SIZES[$i]}"
  target/release/profile_storage --root-path ${ROOT}/tmp_dir --out-path out_profile.jsons --num-trials 64 --num-files 1 --file-size 1073741824 --content random_constant --num-readsets 1 --file-picking random --read-mode sequential --num-read-pages 1 --read-page-size ${read_size} --read-method batch_sequential
done
rm -d ${ROOT}/tmp_dir