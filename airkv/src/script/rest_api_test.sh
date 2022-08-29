export AZURE_ACCOUNTNAME=
export AZURE_ACCOUNTKEY=

# the container and append blob must exist before running this script
CONTAINER_NAME="test-restapi"
BLOB_NAME="meta_0"

../target/debug/rest_api az:///${CONTAINER_NAME}/${BLOB_NAME} 50 > azure_api_50.log