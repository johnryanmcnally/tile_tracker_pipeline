<h1/>tile_project</h1>
A data pipeline to store and enrich Tile Tracker location data, built with Apache Airflow and PostgreSQL. For me, taking a career break did not mean taking a break from learning and keeping my skills sharp. I made time to read, code, and watch videos to upskill myself. This project is one of the results of that time spent.

<h2/>Description</h2>
This project has two main objectives:

1. Record location data throughout a year long trip travelling through Asia and South America
2. Learn and develop personal skills: Python, SQL, Docker, Airflow, API Calls

<h3/>Data Sources</h3>

*Tile Tracker Location Data* ([pytile](https://pypi.org/project/pytile/2.0.1/)) - This is the primary source of data for this project. This is an unofficial Tile API, since Tile does not provide its own. It provides datetime, latitude, and longitude data for each of the user's Tile trackers.

*Location Information* ([GoogleMaps Geocoding API](https://developers.google.com/maps/documentation/geocoding)) - This API call uses the location data to return possible addresses and location tags (airport, place of worship, etc.) which are close to the latitude and longitude.

*Weather Data* ([OpenMeteo API](https://open-meteo.com/)) - This API returns hourly weather data for a day for a given latitude and longitude. For each day of the trip, the average latitude and longitude were calculated then passed to the API. The returned weather data is merged to the location data using the hour in the datetime column.

*Apple Health Data* - This is manually downloaded from the iPhone and provides various health data (walking distance, flights climbed, etc.). This data is not included in the pipeline and is used more for ground truths in toy ML models.

<h3/>Pipeline Details</h3>
<img width="1905" height="590" alt="image" src="https://github.com/user-attachments/assets/c1e7cac1-c527-462f-a56f-229d7f17339d" />

This image is a high-level map of the enire pipeline. The data originates from the Tile tracker. To reach Tile's database, the tracker sends data through the companion iPhone app. This is where the pipeline, built in Apache Airflow begins. Below shows the DAG from Airflow.

<img width="2013" height="460" alt="tile_dag-graph (3)" src="https://github.com/user-attachments/assets/c74c25dd-0bea-4723-98ef-f8a814324cf0" />

There are 5 tasks in the DAG detailed below:

*extract_tile_data* ([code](data_handling/)) - This task handles the API call to the Tile database to retrieve the raw data for all trackers and stores it in a permanent 'raw' data folder in JSON format. This folder is currently on my local machine, but a duplicate API call happens in AWS to load into an S3 bucket.

*feature_engineering* ([code](data_handling/)) - This task loads the raw JSON files and extracts the data for the 'John' tracker, which is the tracker I carry daily. The task then creates a few feature columns and trains an HDBScan model to cluster the latitude and longitude coordinates using the Haversine distance metric (distance on a sphere). The raw cluster labels are then refined using a 'direction similarity' feature, which analyzes how similar the direction (or bearing) of clusters are. If the similarity is above a threshold, the cluster gets replaced with -3, which indicates this cluster is likely during transit. Refining the clusters in this manner reduces the number of API calls in the following tasks, since the locations in transit will not give meaningful results.

*reverse_geocode* ([code](data_handling/reverse_geocode.py)) - This task handles the API call to GoogleMaps Geocoding API. The mean latitude and longitude for each cluster is sent and the API returns possible addresses, place_ids (Google's internal id for a place), and location tags. The data is then processed to assign the first address returned to the cluster and all place_ids and location tags are stored in a list linked to the cluster label. The data are stored in separate parquet files in a temporary location for loading to PostgreSQL database in the following step.

*retrieve_weather* ([code](data_handling/retrieve_weather.py)) - This task calls the OpenMeteo API to return the hourly weather data for the average location of each day. First the data is grouped by day, taking the mean of the latitude and longitude. The means for the days are passed to the API, and it returns the hourly weather for that location. The hourly weather data is then merged with the original location data on the 'hour' from the datetime. The merged data is saved to a temporary location in a parquet file to be loaded to the database.

*postgres_load*  ([code](data_handling/postgres_load.py)) - This task loads all of the parquet files from the temporary file location into respective tables in a PostgreSQL database using sqlalchemy.
