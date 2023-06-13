#!/bin/bash
export PROJECT_PREFIX="analytics-kris_do_nonprod"

# Shared Environment Variables
export CFN_ENVIRONMENT_MIN="${CFN_ENVIRONMENT/datasvcs/}"
export PROJECT_PREFIX="${PROJECT_PREFIX}"
export CFN_IMAGE_RELEASE="${CFN_IMAGE_RELEASE:-latest}"
export CFN_TEMPLATE_DIR="cloudformation/templates"
export NONPRIV_ROLE_NAME="infra-cfnrole-analytics-nonprivileged"
export PARAMS_FILE="cloudformation/params/datasvcs.yml"
CFN_RAND_STRING="$(date '+%s' | base64)"
export CFN_RAND_STRING


# Stack Details
export CFN_ECR_STACK="${PROJECT_PREFIX}-stack-ecr"
