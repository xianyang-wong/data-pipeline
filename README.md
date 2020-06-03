# Data Pipeline

## A) Projects
### JobsBank SG
On a daily basis, this repo would extract and save all new job postings and save the information to the database.The SQL code to create the table to store the extracted data is found in `tasks/jobsbanksg/jobs_bank_api.sql`.

## B) Running DAGs on Airflow
1) Pull Airflow image from DockerHub

    `docker pull puckel/docker-airflow`

2) Run docker container while copying local directory to container

    `docker run -d -p 8080:8080 --name airflow-webserver puckel/docker-airflow webserver`

3) Open up docker shell

    `docker exec --user="root" -it airflow-webserver /bin/bash`

4) Install VIM & Git

    `apt-get update`  
    `apt-get install vim`  
    `apt-get install git`  
    
5) Git clone data-pipeline repository

    `git clone https://github.com/xianyang-wong/data-pipeline.git`
    
6) Create new config.ini file in cloned data-pipeline/config folder using template shown below

    `[aws_db]`  
    `host=<host>`  
    `port=<port>`  
    `user=<username>`  
    `password=<password>`  
    `database=<database>`  

7) Modify airflow.cfg

    from `dags_folder = /usr/local/airflow/dags` to `dags_folder = /usr/local/airflow/data-pipeline/dags`

8) Install additional python libraries

    `pip install tqdm urllib3==1.25.9`

9)  Run airflow scheduler

    `airflow scheduler`
    
10) Access Airflow UI

    `http://localhost:8080/admin/`

