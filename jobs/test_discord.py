
import os
import json
import urllib.request

webhook = os.environ["LAKEFLOW_DISCORD_WEBHOOK"]

payload = {
    "content": "LakeFlow test: Discord webhook is working!"
}

data = json.dumps(payload).encode("utf-8")

request = urllib.request.Request(
    webhook,
    data=data,
    headers={
    "Content-Type": "application/json",
    "User-Agent": "LakeFlow-Monitor/1.0"
},
    method="POST"
)

with urllib.request.urlopen(request, timeout=10) as response:
    print("Discord status:", response.status)

print("Test message sent!")
