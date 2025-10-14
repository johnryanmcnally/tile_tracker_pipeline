# Native
__import__('pysqlite3') 
import sys
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
import sqlite3
import os
import getpass
import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Third Party
from langchain_chroma import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_core.documents import Document
from dotenv import load_dotenv
import pandas as pd



print("setting up...")
load_dotenv()
os.environ["GOOGLE_API_KEY"] = os.getenv("GOOGLE_API_KEY")

if "GOOGLE_API_KEY" not in os.environ:
    os.environ["GOOGLE_API_KEY"] = getpass.getpass("Enter your Google API Key: ")

# setup db location
db_location = "./data_dashboard/data/chroma_db"
# add_documents = not os.path.exists(db_location)
api_key = os.getenv("GOOGLE_API_KEY")
embeddings = GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-001", google_api_key=api_key, transport="grpc")

print("Creating new vector store and adding documents...")
documents = []
ids = []

with open('./LLM_chatbot/finetune_data/joya_journal.json', 'r') as f:
    journal = json.load(f)

for i, entry in enumerate(journal):
    narrative_summary = f"""
Trip Segment {entry['Trip Segment']} 
Country: {', '.join(entry['Country'])}
Date: {entry['Date']}
Journal Entry: {entry['Entry']}
""" 
    document = Document(
        page_content=narrative_summary,
        metadata={
            "id": str(i),
            "Trip Segment": entry['Trip Segment'],
            "Date": entry['Date'],
            "Country": entry['Country'][0]
        }
    )
    ids.append(str(i))
    documents.append(document)

vector_store = Chroma.from_documents(
    documents=documents,
    embedding=embeddings,
    persist_directory=db_location,
    collection_name='tile_data'
)
logger.info(f"Vector store created and saved to {db_location}")

retriever = vector_store.as_retriever(search_kwargs={"k": 10} )
print(retriever.invoke("Korea"))