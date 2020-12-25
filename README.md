Apache Airflow is used to build a data pipeline for a fictitious company called Sparkify, that needs to process song, artist, user and song play data for analytical purposes.
The DAG created in Airflow includes operators for creating staging tables in Redshift (using data from S3), loading on to fact and dimension tables, as well as for performing data quality checks.
