sudo docker run --env-file .env \
    -p 8080:8080 -p 8081:8081 \
    -it executor-image:latest