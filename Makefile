project_id ?= $(shell bash -c 'read -p "project_id: " project_id; echo $$project_id')
region ?= $(shell bash -c 'read -p "region: " region; echo $$region')
credentials ?= $(shell bash -c 'read -p "region: " region; echo $$region')
prefect_key ?=$(shell bash -c 'read -p "prefect_key: " prefect_key; echo $$prefect_key')
prefect_workspace ?=$(shell bash -c 'read -p "prefect_workspace: " prefect_workspace; echo $$prefect_workspace')
api ?= $(shell bash -c 'read -p "api: " api; echo $$api')

prepare:
	echo -e "Create gcs bucket. Block name "google" and gcs bucket must de-project_$(project_id)"
	prefect block create gcs-bucket
	echo "pip install googlemaps">init_script.sh
	echo "pip install prefect">>init_script.sh
	echo "pip install prefect-gcp">>init_script.sh
	echo "pip install prefect-gcp['cloud_storage']">>init_script.sh
	echo "prefect cloud login --key $(prefect_key) --workspace $(prefect_workspace)">>init_script.sh
	echo "echo "export API=${api}" | tee -a /etc/profile">>init_script.sh
	echo "source /etc/profile">>init_script.sh

terraform:
	terraform init
	terraform apply -var "project_id=$(project_id)" -var "region=$(region)" -var "credentials=/home/yusuf/google/.google/google1.json"



flow:
	python3 flow.py --project_id=$(project_id) --region=$(region)


run: prepare terraform flow
