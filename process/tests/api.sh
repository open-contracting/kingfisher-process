#/bin/bash
curl 'http://127.0.0.1:22061/api/create_collection' \
  -H 'Connection: keep-alive' \
  -H 'DNT: 1' \
  -H 'accept-language: en-US,en;q=0.9,cs;q=0.8,de;q=0.7,sk;q=0.6,it;q=0.5' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36' \
  -H 'Content-Type: text/plain;charset=UTF-8' \
  -H 'Accept: */*' \
  -H 'Origin: null' \
  -H 'Sec-Fetch-Site: cross-site' \
  -H 'Sec-Fetch-Mode: no-cors' \
  -H 'Sec-Fetch-Dest: empty' \
  --data-binary '{"source_id": "fdfsdfsfssf", "data_version": "2020-01-15 10:00:02", "upgrade": true}' \
  --compressed