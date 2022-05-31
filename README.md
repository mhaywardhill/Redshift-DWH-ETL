# S3 to Redshift ETL pipeline

A music streaming startup, Sparkify, has grown its user base and song database and wants to move its processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app and a directory with JSON metadata on the songs in their app.

In this project, I build an ETL pipeline that extracts data from S3, stages it in Redshift, and then transforms the data into a set of dimensional tables ready for their analytics team to continue finding insights into the songs users are listening to.

## Setup the Environment

### 1. Set environments variables:

```
HISTCONTROL=ignoreboth
 export DWH_ENDPOINT=<Redshift endpoint>
 export DWH_DB_PASSWORD=<Redshift password>
```

### 2. Setup the Python virtual environment:

```
conda create -n sparkify python=3.9
conda activate sparkify
make install
```  

### 3. Create tables in Redshift

```
python ./create_tables.py
```

### 4. Copy data from S3 to Redshift

```
python ./etl.py
```



## Cleanup Resources

```
conda deactivate
conda env remove -n sparkify
```

