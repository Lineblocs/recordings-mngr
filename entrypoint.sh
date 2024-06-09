if [[ -z "${ARI_RECORDING_APP}" ]]; then
   export ARI_RECORDING_APP="lineblocs-recordings"
fi
if [[ -z "${S3_BUCKET}" ]]; then
   export S3_BUCKET="lineblocs"
fi
if [[ -z "${MEDIA_API_URL}" ]]; then
   API_SCHEME="https"
   export MEDIA_API_URL="${API_SCHEME}://mediafs.${DEPLOYMENT_DOMAIN}"
fi

API_SCHEME="http"
DEPLOYMENT_DOMAIN="${DEPLOYMENT_DOMAIN:-example.org}"
if [[ -z "${API_URL}" ]]; then
   export API_URL="${API_SCHEME}://internals.${DEPLOYMENT_DOMAIN}"
fi

echo "Starting recordings manager"
./main