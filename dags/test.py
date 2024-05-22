import requests

url = 'https://restcountries.com/v3/all'
response = requests.get(url)
data = response.json()
for d in data:
    print(d["name"]["official"])
    print(d["population"])
    print(d["area"])