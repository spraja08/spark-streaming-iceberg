all: test build

test: pytest tests

build:
	rm -rf ./dist && mkdir ./dist
	cp ./main.py ./dist
	cp ./config.json ./dist
	zip -r dist/jobs.zip jobs
	zip -r dist/shared.zip shared
	docker run --rm -v $(PWD):/foo -w /foo lambci/lambda:build-python3.7 \
	pip install -r requirements.txt -t ./dist/libs
	cd ./dist/libs && zip -r -D ../libs.zip .
	cd ./dist && aws s3 sync . s3://rspamzn-retail-app/
	#echo "spark-submit --py-files jobs.zip,shared.zip,libs.zip --files config.json main.py --job retail" > ./dist/run.sh 
	echo 'aws emr add-steps --cluster-id j-258HYT78FYMWG --steps Type=Spark,ActionOnFailure=CONTINUE,Args=[--py-files,\"s3://rspamzn-retail-app/jobs.zip,s3://rspamzn-retail-app/shared.zip,s3://rspamzn-retail-app/libs.zip\",--files,s3://rspamzn-retail-app/config.json,s3://rspamzn-retail-app/main.py,--job,retail-stage0,--app_name,RetailCDP,--source_data_path,s3://rspamzn-retail-dataset/inputs/,--output_data_path,s3://rspamzn-retail-dataset/outputs/]' > ./dist/0-emr-run.sh
	echo 'aws emr add-steps --cluster-id j-258HYT78FYMWG --steps Type=Spark,ActionOnFailure=CONTINUE,Args=[--py-files,\"s3://rspamzn-retail-app/jobs.zip,s3://rspamzn-retail-app/shared.zip,s3://rspamzn-retail-app/libs.zip\",--files,s3://rspamzn-retail-app/config.json,s3://rspamzn-retail-app/main.py,--job,retail-stage1,--app_name,RetailCDP,--source_data_path,s3://rspamzn-retail-dataset/inputs/,--output_data_path,s3://rspamzn-retail-dataset/outputs/]' > ./dist/1-emr-run.sh
	echo 'aws emr add-steps --cluster-id j-258HYT78FYMWG --steps Type=Spark,ActionOnFailure=CONTINUE,Args=[--py-files,\"s3://rspamzn-retail-app/jobs.zip,s3://rspamzn-retail-app/shared.zip,s3://rspamzn-retail-app/libs.zip\",--files,s3://rspamzn-retail-app/config.json,s3://rspamzn-retail-app/main.py,--job,retail-stage2,--app_name,RetailCDP,--source_data_path,s3://rspamzn-retail-dataset/inputs/,--output_data_path,s3://rspamzn-retail-dataset/outputs/]' > ./dist/2-emr-run.sh
	echo 'aws emr add-steps --cluster-id j-258HYT78FYMWG --steps Type=Spark,ActionOnFailure=CONTINUE,Args=[--py-files,\"s3://rspamzn-retail-app/jobs.zip,s3://rspamzn-retail-app/shared.zip,s3://rspamzn-retail-app/libs.zip\",--files,s3://rspamzn-retail-app/config.json,s3://rspamzn-retail-app/main.py,--job,retail-stage3,--app_name,RetailCDP,--source_data_path,s3://rspamzn-retail-dataset/inputs/,--output_data_path,s3://rspamzn-retail-dataset/outputs/]' > ./dist/3-emr-run.sh
	#chmod +x ./dist/run.sh 
	chmod +x ./dist/0-emr-run.sh
	chmod +x ./dist/1-emr-run.sh
	chmod +x ./dist/2-emr-run.sh
	chmod +x ./dist/3-emr-run.sh