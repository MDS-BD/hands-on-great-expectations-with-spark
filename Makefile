build: ## build Docker image
	docker build --no-cache -t mediaset-data-quality-jupyter-dev .

run: ## run Docker container (dev env) with a Jupyter Notebook instance
ifeq ($(OS),Windows_NT)
	docker run --rm -d -p 8888:8888 --user root -e GRANT_SUDO=yes -v ${CURDIR}:/home/jovyan/work/ --name dq-dev-notebook mediaset-data-quality-jupyter-dev
else
	docker run --rm \
	-p 8888:8888 \
	--user root \
	-e GRANT_SUDO=yes \
	-v $$(pwd):/home/jovyan/work/ \
	--name dq-dev-notebook \
	mediaset-data-quality-jupyter-dev
endif

validate-data: ## run the Data Quality Validation
ifeq ($(OS),Windows_NT)
	docker run --rm --user root -e GRANT_SUDO=yes -v ${CURDIR}:/home/jovyan/work/ -w /home/jovyan/work/data_quality/validate_data --name dq-validation-run mediaset-data-quality-jupyter-dev spark-submit data_validation_with_checkpoints.py --log_level info --dataset_name sample_data --suite_name data_quality_check
else
	docker run --rm \
	--user root \
	-e GRANT_SUDO=yes \
	-v $$(pwd):/home/jovyan/work/ \
	-w /home/jovyan/work/data_quality/validate_data \
	mediaset-data-quality-jupyter-dev \
	spark-submit data_validation_with_checkpoints.py \
	--log_level info --dataset_name sample_data --suite_name data_quality_check
endif

ge-doc: ## generate Great Expectations Suites Data Documentation
ifeq ($(OS),Windows_NT)
	docker run -v ${CURDIR}:/app/src -w /app/src mediaset-data-quality-jupyter-dev python3 ./data_quality/generate_data_doc/generate_expectation_suite_doc_site.py
else
	docker run \
	-v ${CURDIR}:/app/src \
	-w /app/src \
	mediaset-data-quality-jupyter-dev \
	python \
	./data_quality/generate_data_doc/generate_expectation_suite_doc_site.py
endif