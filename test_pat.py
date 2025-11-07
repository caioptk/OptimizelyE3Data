import requests

headers = {
    "Authorization": "Bearer 2:a0rbvJ5r62CqkQqg0BS8-eZRSaM3hNY9SUbjw0om9w6tCPBh7zzE"
}

url = "https://api.optimizely.com/v2/export/credentials?duration=1h"

resp = requests.get(url, headers=headers)

print("Status Code:", resp.status_code)
print("Response:", resp.text)
