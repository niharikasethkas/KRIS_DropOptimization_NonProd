#!/bin/bash

set -euxo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
. scripts/cfn_vars.sh

cfn_manage deploy-stack \
    --stack-name "$CFN_ECR_STACK" \
    --template-file "${CFN_TEMPLATE_DIR}/ecr.yml" \
    --parameters-file "$PARAMS_FILE" \
    --role-name "$NONPRIV_ROLE_NAME"