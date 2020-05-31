# data-pipeline

### Running DAGs on Airflow
1) Pull Airflow image from DockerHub

    `docker pull puckel/docker-airflow`

2) Run docker container while copying local directory to container

    `docker run -d -p 8080:8080 -v "D:/PyCharm Projects/data-pipeline/:/usr/local/airflow/data-pipeline" --name airflow-webserver puckel/docker-airflow webserver`

3) Open up docker shell

    `docker exec --user="root" -it airflow-webserver /bin/bash`

4) Install VIM

    `apt-get update` and `apt-get install vim`

5) Modify airflow.cfg

    from `dags_folder = /usr/local/airflow/dags` to `dags_folder = /usr/local/airflow/data-pipeline/dags`

6) Install additional python libraries

    `pip install tqdm urllib3==1.25.9`

7)  Run airflow scheduler

    `airflow scheduler`
    
8) Access Airflow UI

    `http://localhost:8080/admin/`

Miscellaneous: Other Docker Commands

    docker ps -a
    docker stats
    docker container stop airflow-webserver
    docker rm airflow-webserver