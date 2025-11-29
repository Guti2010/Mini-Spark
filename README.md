Comando para iniciar el spark:

- docker compose up --build master worker --scale worker=3
  Coamndos para probar el spark:
- docker compose run --rm client word-count <file.txt>
- docker compose run --rm client status <JOB_ID>
- docker compose run --rm client results <JOB_ID>

- Las pruebas de los operadores se ejecutan así dentro del contenedor:
  cargo test -p common --lib

acceder al contenedor desde powershell:
docker run --rm -it `  -v "${PWD}:/app"`
-w /app `  rust:1.80`
bash

- Las pruebas de Cover:
  cargo install cargo-llvm-cov --version 0.6.15
  cargo llvm-cov -p common --lib --tests
