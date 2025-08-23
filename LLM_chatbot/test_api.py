import requests
import json

# Test Locally
# url = "http://127.0.0.1:8000/prompt"
# question = 'tell me about Vietnam'

# params = {'question':question}
# response = requests.get(url=url, params=params)
# print(response)
# print(json.loads(response.text))

# Test Docker
url = "http://localhost:8000/prompt"
question = 'tell me about Vietnam'

params = {'question':question}
response = requests.get(url=url, params=params)
print(response)
print(json.loads(response.text))