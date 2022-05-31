#  ETL pipeline, S3 to Redshift

Build an ETL pipeline that extracts data from S3, stages it in Redshift, and then transforms the data into a set of dimensional tables ready for analytics.

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

### 3. Copy data from S3 to Redshift

```
python ./etl.py
```



## Cleanup Resources

```
conda deactivate
conda env remove -n sparkify
```

