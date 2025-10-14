# Third Party
from langchain_chroma import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_core.documents import Document
from dotenv import load_dotenv
import pandas as pd

# Native
import sqlite3
import os
import getpass

print("setting up...")
load_dotenv()
os.environ["GOOGLE_API_KEY"] = os.getenv("GOOGLE_API_KEY")

if "GOOGLE_API_KEY" not in os.environ:
    os.environ["GOOGLE_API_KEY"] = getpass.getpass("Enter your Google API Key: ")

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
print("pulling data...")
df = pd.read_sql(query, con=engine)
columns = df.columns
# setup db location
db_location = "./data_dashboard/data/chroma_langchain_db"
add_documents = not os.path.exists(db_location)
embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")

print("Creating new vector store and adding documents...")
documents = []
ids = []

for i, row in df.iterrows():
    narrative_summary = (
        f"While on their trip, Maya and John visited {row['country']}. "
        f"The location was a {row['tag']} in the cluster {row['cluster_label']}. "
        f"The weather was {row['temperature']}°C, with {row['cloud_cover']}% cloud cover and {row['precipitation']}mm of precipitation. "
        f"They were at a latitude of {row['latitude']} and a longitude of {row['longitude']} "
        f"on {row['date']}."
    )

    document = Document(
        page_content=narrative_summary,
        metadata={
            "id": str(i),
            "country": row['country'],
            "latitude": row['latitude'],
            "longitude": row['longitude'],
            "date": row['date'],
            "time": row['time'],
            "tag": row['tag'],
            "cluster_label": row['cluster_label'],
            "temperature": row['temperature'],
            "relative_humidity": row['relative_humidity'],
            "cloud_cover": row['cloud_cover'],
            "precipitation": row['precipitation'],
            "elevation": row['elevation']
        }
    )
    ids.append(str(i))
    documents.append(document)

vector_store = Chroma.from_documents(
    documents=documents,
    embedding=embeddings,
    persist_directory=db_location
)
print(f"Vector store created and saved to {db_location}")