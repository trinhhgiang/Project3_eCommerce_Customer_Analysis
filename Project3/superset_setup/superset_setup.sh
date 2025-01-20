# !/bin/bash


### Superset

docker-compose -f ./docker_compose_setup/docker-compose.yml exec superset superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email vulamanhclc2003@gmail.com \
    --password admin

docker-compose -f ./docker_compose_setup/docker-compose.yml exec superset pip install sqlalchemy-trino
docker-compose -f ./docker_compose_setup/docker-compose.yml restart superset
docker-compose -f ./docker_compose_setup/docker-compose.yml exec superset superset db upgrade

docker-compose -f ./docker_compose_setup/docker-compose.yml exec superset superset init


# trino://'':@trino:8080/cassandra



