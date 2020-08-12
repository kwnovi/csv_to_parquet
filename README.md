
  

#### Introduction
This python script convert a given product dataset from CSV to Parquet.

We suppose there is a new dataset everyday. The script will output two parquet files separating the valid products from the invalid ones : `product_catalog_valid.parquet` and `product_catalog_errors.parquet`

#### Requirements
	pyarrow

#### Usage
	python csv_to_parquet.py -f inputs/product_catalog.csv
 

#### Benchmark

The inital product dataset was concatenated to test the overwall script performance.
	
	product_catalog.csv (1k products)
	Execution time : 0.45s

	--

	product_catalog_200k.csv (~200k products)
	Execution time : 8s

	--

	product_catalog_2m.csv (~2m products)
	Execution time : 1min45s


I could not add `product_catalog_200k.csv` and  `product_catalog_2m.csv` to the GIT repository because of GitHub's file size limit but the `test.py` script can generate fake data on the fly.

#### Testing

I wrote two tests to ensure image validation and the overwall execution time of the script. More tests could be written (to check data type for instance) 

	
#### Scalability

Another solution could have been to directly use PySpark to ingest the CSV file, do the checks and write the parquet files. We could have then created a dedicated Dataproc cluster for the script. In the end, a DAG on Airflow would trigger a Dataproc cluster and the script everyday.

  

*But..*

  

I would argue that in this case and considering the benchmark, it is best to implement the script in a Cloud function triggered each time a new dataset is uploaded in a bucket. This way, a dataset would be processed and available as soon as possible. Also the processing would be parallelized if there is a product dataset by provider/seller.

  

However, it would be necessary to switch to a PySpark script if the dataset exceed 10 millions of products (Cloud functions timeout after 9min)