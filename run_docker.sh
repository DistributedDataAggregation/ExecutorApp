sudo docker run --env-file .env \
    -p 8080:8080 -p 8081:8081 \
    -v /home/karol/data:/data \
    -it executor-image:latest