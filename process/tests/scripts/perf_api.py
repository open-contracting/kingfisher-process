from datetime import datetime

import requests

"""
Basic performance test of api throughput for create_collection_file.
"""

time = datetime.now()
data = {
            "source_id": "perf test",
            "data_version": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "note": "perf test",
            "sample": False,
            "compile": True,
            "upgrade": True,
            "check": True,
        }

response = requests.post("{}/{}".format("http://localhost:8101", "api/v1/create_collection"), json=data)
response_data = response.json()
collection_id = response_data["collection_id"]

start = datetime.now()
count = 10000

for i in range(0, count):
    data = {
            "collection_id": collection_id,
            "path": "/tmp/{}{}".format(i, datetime.now().timestamp()),
            "file_name": "file_name",
            "url": "url"
        }
    response = requests.post("{}/{}".format("http://localhost:8101", "api/v1/create_collection_file"), json=data)

print(count/(datetime.now() - start).total_seconds())
