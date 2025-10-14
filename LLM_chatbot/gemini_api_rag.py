import os
import getpass
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_chroma import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings, ChatGoogleGenerativeAI
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document
from dotenv import load_dotenv
import sqlite3
import pandas as pd

# --- Prerequisites ---
# Before running, you must install the required libraries:
# pip install -qU langchain-text-splitters langchain-chroma langchain-google-genai

# And set your Google API key as an environment variable:
# In your terminal (macOS/Linux):
# export GOOGLE_API_KEY="YOUR_API_KEY"
# In your terminal (Windows):
# set GOOGLE_API_KEY="YOUR_API_KEY"
load_dotenv()
os.environ["GOOGLE_API_KEY"] = os.getenv("GOOGLE_API_KEY")

# --- 3. Create Embeddings and Vector Store ---
# We use Google's embedding model to convert the text chunks into numerical vectors.
# Then, we store them in a vector store for efficient search.
# Chroma is an in-memory vector store perfect for this kind of example.
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
df = pd.read_sql(query, con=engine)
columns = df.columns
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




embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
# vector_store = Chroma.from_documents(documents=document_chunks, embedding=embeddings)
vector_store = Chroma(
    collection_name="tile_data",
    persist_directory=db_location,
    embedding_function=embeddings
)

if add_documents:
    vector_store.add_documents(documents=documents, ids=ids)

print("3. Vector store created with embeddings.")

# --- 4. Define the RAG Pipeline ---
# This is the core of the RAG system.
# a. Define the prompt template for the LLM.
#    The `{context}` variable will be populated by the retrieved documents.
#    The `{input}` variable will be the user's question.
prompt = ChatPromptTemplate.from_template("""
    You are a married couple, named Maya and John. Maya and John are on a gap year where they are travelling around the world. 
    You tell stories of their trip in first person using the provided location and weather data.
    Do not report the data, only use it to create a story.
    Do not ask for follow up questions.
    The location data includes 'latitude' and 'longitude' as well as 'cluster_label' which group similar points together.
    The 'tag' is a label from GoogleMaps which tries to assign the type of location to the 'cluster_label'.

    Here is the relevant data: 
    <context>
    {context}
    </context>
                                          
    Here is the question to answer: {input}
    """)

# b. Initialize the Gemini LLM for generation.
llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash")

# c. Create the retriever.
#    This component will fetch the most relevant document chunks from the vector store.
retriever = vector_store.as_retriever()

# d. Create the document combining chain.
#    This chain takes the retrieved documents and stuffs them into the prompt.
document_chain = create_stuff_documents_chain(llm, prompt)

# e. Create the final retrieval chain.
#    This chain combines the retriever and the document chain into a single pipeline.
rag_chain = create_retrieval_chain(retriever, document_chain)

print("4. RAG pipeline created successfully.")

# --- 5. Run the RAG Pipeline with a User Question ---
print("\n--- Running RAG query ---")
question = "Tell me about your time in Vietnam."
print(f"User Question: '{question}'")

response = rag_chain.invoke({"input": question})

print("\n--- Answer ---")
print(response["answer"])

print("\n--- Running another RAG query ---")
question = "How long did you spend in Thailand"
print(f"User Question: '{question}'")

response = rag_chain.invoke({"input": question})

print("\n--- Answer ---")
print(response["answer"])
