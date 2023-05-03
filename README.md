# Istanbul Traffic Density | Data Engineering Zoomcamp

This project is requirement of data engineering zoomcamp. Building cloud services infrastructure, creating data ingestion pipeline, transforming data and as a reporting creating data dashboard.

## Project info

In this project we are looking Istanbul traffic density. The data I used in the project was published by Istanbul City Hall open data platform. The content of the data is the traffic density of 5014 unique locations. Data was collected hourly for 3 (2019,2021,2022) years. Comparing the hours of the day, month and years and visualize locations on the looker dashboard with google maps integrations.
Here is raw data sample:

| date_time 	    | longitude 	 | latitude 	  | geohash 	| minimum_speed 	| maximum_speed 	| average_speed 	| number_of_vehicles 	|
|-----------	    |-----------	 |----------	  |---------	|---------------	|---------------	|---------------	|--------------------	|
|2020-01-01T00:00:00|28.8116455078125|41.0806274414063|sxk3xw      	|134              	|18               	|81               	|132                    |
|2020-01-01T00:00:00|29.1082763671875|40.9872436523438|sxk9nm     	|143             	|10               	|73               	|162                    |
|2020-01-01T00:00:00|29.0972900390625|41.0037231445313|sxk9q0   	|128              	|6               	|50               	|110                    |


## Processing of Data

For processing data I've extracted hours, month  and year information from timestamp column and saved as individuals columns. Google looker studio accept geographic locations with "(latitude, longitude)" as format. I've concatenated latitude and logitude columns. Unfortunately data source does not provide geohash counterparts. For getting locations address I used google maps python library and saved location addresses another column. Istanbul has 39 district. For comparing each district I've extracted district information from address column. With processed data I can compare each hour, each month for 3 year.



|          date_time|year|month|hour|minimum_speed|maximum_speed|average_speed|number_of_vehicles|            district|geohash|         coordinates|            location|
|-------------------|----|-----|----|-------------|-------------|-------------|------------------|--------------------|-------|--------------------|--------------------|
|2022-08-05 03:00:00|2022|    8|   0|           81|           11|           41|                28|Eyüpsultan/İstanb...| sxk9d3|41.05316162109376...|Eyüp Merkez, 3405...|
|2022-08-05 03:00:00|2022|    8|   0|          109|           10|           65|                89|Üsküdar/İstanbul,...| sxk9m6|41.01470947265625...|Küçük Çamlıca, Çi...|
|2022-08-05 03:00:00|2022|    8|   0|          168|           13|           68|               245|Esenyurt/İstanbul...| sxk3s9|41.05316162109376...|Ardıçlı Mh., Yase...|

## Transforming of Data

For transforming data I created 5 dataframe. Transformed and processed data was saved to bigquery. Processed data was partitioned on a monthly basis.

Average dataframe: With this dataframe I grouped coordinates, hour, month and year columns. While grouping calculated average of all numeric variables for each group. Hours, months and years can be visualized as time series for each location.

District dataframe: With this dataframe I grouped district, year and month colums. While grouping calculated average of all numeric variables for each group. Months and years can be visualized as time series for each district.


## Technology Stack:

* Data Cluster: Dataproc
* Data Lake: Google Cloud Storage
* Data Warehouse: Google BigQuery
* Data Dashboard: Google Looker Studio
* Orchestrating: Prefect and Prefect Cloud
* Frameworks: Spark, Pyspark jobs
