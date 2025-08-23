from langchain_ollama import OllamaEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document
import os
import pandas as pd
import sqlite3

engine = sqlite3.connect("data_dashboard/data/dashboard_data.sqlite")
query = f"""
WITH rank AS (
    SELECT 
        t.cluster_label,
        tdj.date,
        tdj.time,
        tdj.latitude,
        tdj.longitude,
        w.elevation_meters_asl AS elevation,
        w.temperature_2m AS temperature,
        w.relative_humidity_2m AS relative_humidity,
        w.cloud_cover,
        w.precipitation,
        t.tag,
        ca.country,
        ROW_NUMBER() OVER(PARTITION BY t.cluster_label ORDER BY tdj.date DESC, tdj.time DESC) AS rn
    FROM tags AS t 
    INNER JOIN tile_data_john AS tdj 
        ON t.cluster_label = tdj.cluster_label
    INNER JOIN weather AS w
        ON t."index" = w."index"
    INNER JOIN cluster_address as ca
		ON t.cluster_label = ca.cluster_label
    WHERE 
        t.tag NOT IN ('street_address','plus_code','route','premise','subpremise','establishment','point_of_interest')
)

SELECT
    cluster_label,
    date,
    time,
    country, 
    latitude,
    longitude,
    elevation,
    temperature,
    relative_humidity,
    cloud_cover,
    precipitation,
    tag
FROM rank
WHERE rn = 1;
"""
df = pd.read_sql(query, con=engine)
columns = df.columns
embeddings = OllamaEmbeddings(model="nomic-embed-text")
# setup db location
db_location = "./LLM_chatbot/chroma_langchain_db"
add_documents = not os.path.exists(db_location)

if add_documents:
    documents = []
    ids = []

    for i, row in df.iterrows():
        document = Document(
            page_content = (
            f"Maya and John were in {row['country']} at latitude {row['latitude']} and longitude {row['longitude']} "
            f"on {row['date']} at {row['time']} "
            f"The location is a {row['tag']} and is in the cluster {row['cluster_label']}."
            f"While in cluster {row['cluster_label']} the weather conditions were: "
            f"Temperature: {row['temperature']}°C, "
            f"Relative Humidity: {row['relative_humidity']}%, "
            f"Cloud Cover: {row['cloud_cover']}%, "
            f"Precipitation: {row['precipitation']}mm, "
            f"Elevation: {row['elevation']} meters. "
            ),
            id = str(i)
        )
        ids.append(str(i))
        documents.append(document)

vector_store = Chroma(
    collection_name="tile_data",
    persist_directory=db_location,
    embedding_function=embeddings
)

if add_documents:
    vector_store.add_documents(documents=documents, ids=ids)

retriever = vector_store.as_retriever(
    search_kwargs = {"k": 100}
)