# Third Party
from fastapi import FastAPI
from magnum import Magnum # for AWS Lambda

# Custom
import functions

app = FastAPI()
handler = Magnum(app) # allows lambda to interact with app

@app.get("/")
def health_check():
    return {"health_check": "OK"}

@app.get("/info")
def info():
    return {'name': 'joya_chatbot',
            'description': 'API to interact with Joya'}

@app.get("/prompt")
def prompt(question: str):
    response = functions.ask_joya(question)
    return response