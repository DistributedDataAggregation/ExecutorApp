# How to run multiple containers
Make sure you have docker and docker-compose

## 1. Prerequisites
You'll need to create folder for data under ***/home/data***
and store files for queries there

## 2. Build the image for executor app
```bash
./build-docker-image.sh
```

## 3. To start containers with compose (in detached state "-d flag")
```bash
docker-compose up -d 
```

## 4. Tear down containers
```bash
docker-compose down
```
