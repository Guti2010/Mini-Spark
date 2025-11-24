Coamndo par ainiciar el spark: 
- docker compose up --build master worker --scale worker=3
Coamndos para probar el spark:
- docker compose run --rm client submit "wc-test"
- docker compose run --rm client status <JOB_ID>
- docker compose run --rm client results <JOB_ID>
