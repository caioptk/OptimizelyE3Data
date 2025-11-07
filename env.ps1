import requests

headers = {
    "Authorization": "Bearer YOUR_PAT_HERE"
}
resp = requests.get("https://api.optimizely.com/v2/accounts", headers=headers)
print(resp.status_code)
print(resp.json())
