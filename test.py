import requests, os
from dotenv import load_dotenv
load_dotenv()

r = requests.get(
    "https://jsearch.p.rapidapi.com/search",
    headers={
        "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    },
    params={"query": "developer in Dubai UAE", "page": "1", "num_pages": "1"},
)
print(r.status_code)
print(r.headers)
print(r.text[:1000])