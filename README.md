Comando para iniciar el spark: 
- docker compose up --build master worker1 worker2

Comandos para probar el spark:
- docker compose run --rm client submit "wc-test"
- docker compose run --rm client status <JOB_ID>
- docker compose run --rm client results <JOB_ID>
