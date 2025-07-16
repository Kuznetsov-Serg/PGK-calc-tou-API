# PGK DRS Platform

## Installation

### Run local

1. Get Python >= 3.10 (https://www.python.org/). Current project version is 3.0.6
2. Clone repo:
3. Go to required project directory. For example:
    ```
    cd ./pgk_report
    ```
4. Dependencies:
    ```
    pip3 install --upgrade -r requirements.txt
    ```
   
### Run local with docker-compose

1. run via command
    ```
    docker-compose up```

2. Rebuild service
   ```
    
   docker-compose up -d --no-deps --build drs_backend drs_worker
   ```

3. Rebuild all infrastructure
   ```
   docker-compose up --build 
   ```
   
### Migration
1. Generate migration
   ```
   alembic revision --autogenerate 
   ``
2. Upgrade database
   ```
   alembic upgrade head
   ```

## Port mapping

range 9980-9990

-9980: Application

-9981: Database

-9982-9985: Rabbit

-9986: Worker

-9987-9990: Reserved

## Components

[x] Database  
[x] MQ  
[x] Migrations  
[x] File system  
[x] Authorization and Registration  
[x] Deploy  
[x] Dictionaries NSI  
[x] Proposal system  
[x] Bid system  
[ ] Search  
[x] Supplier cabinet  
[x] Manager cabinet  
