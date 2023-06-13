#!/bin/bash

set -euxo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091     
. scripts/cfn_vars.sh


CHANAKYA_BASTION_PEM_KEY=$(aws secretsmanager get-secret-value --secret-id CHANAKYA_BASTION_HOST_KEY --region ap-southeast-2 | jq -r .SecretString)
rm -rf bastion-key.pem
cat <<< "$CHANAKYA_BASTION_PEM_KEY" > bastion-key.pem
chmod 600 bastion-key.pem

set -x
CHANAKYA_BASTION_HOST=$(aws secretsmanager get-secret-value --secret-id BASTION_IP --region ap-southeast-2 | jq -r .SecretString)

apk add --update --no-cache openssh

# Copy DAG files to environment
scp -i bastion-key.pem -o StrictHostKeyChecking=no kris_do_nonprod/app/airflow/kris_dropoptimization_airflow_dag.py "$CHANAKYA_BASTION_HOST":efs-mount-point/da/airflow/
rm -rf bastion-key.pem