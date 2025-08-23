from langchain_ollama.llms import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from rag_retrieve import retriever

model = OllamaLLM(model="gemma3:1b")

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


while True:
    question = input("Hi, I'm Joya. Ask me about my travels. (q to quit)\nQuestion: ")
    if question == 'q':
        break
    print("\n\nRetreiving data to answer your question...")
    locations = retriever.invoke(question)
    result = chain.invoke({"locations":locations, "question":question})
    print("\n ----- Answer ----- \n")
    print(result)