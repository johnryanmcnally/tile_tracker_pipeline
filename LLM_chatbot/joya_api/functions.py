# Third Party
from langchain_ollama.llms import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import OllamaEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document
import pandas as pd

# Native
import os
import sqlite3


def ask_joya(question: str):
    model = OllamaLLM(model="gemma3:1b", base_url="http://ollama:11434")

    template = """
    You are a married couple, named Maya and John. Maya and John are on a gap year where they are travelling around the world. 
    You tell stories of their trip in first person using the provided location and weather data.
    Do not report the data, only use it to create a story.
    Do not ask for follow up questions.
    The location data includes 'latitude' and 'longitude' as well as 'cluster_label' which group similar points together.
    The 'tag' is a label from GoogleMaps which tries to assign the type of location to the 'cluster_label'.

    Here is the relevant data: {locations}

    Here is the question to answer: {question}
    """

    prompt = ChatPromptTemplate.from_template(template)
    chain = prompt | model
    retriever = get_retriever()
    locations = retriever.invoke(question)
    result = chain.invoke({"locations":locations, "question":question})
    # print("\n ----- Answer ----- \n")
    return result

def get_retriever():
    embeddings = OllamaEmbeddings(model="nomic-embed-text", base_url="http://ollama:11434")
    db_location = os.environ.get("CHROMA_DB_PATH", "./chroma_langchain_db")

    vector_store = Chroma(
        collection_name="tile_data",
        persist_directory=db_location,
        embedding_function=embeddings
    )

    retriever = vector_store.as_retriever(
        search_kwargs = {"k": 10}
    )
    return retriever

def add_embeddings():
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