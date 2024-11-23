# How to run multiple containers
Make sure you have docker and docker-compose

### Prerequisits
You'll need to create folder for data under /home/data 
and store files for queries there

## 1. Build the image for executor app
```bash
./build-docker-image.sh
```

## 2. To start containers with compose (in detatched state "-d flag")
```bash
docker-compose up -d 
```

## 3. Tear down containers
```bash
docker-compose down
```