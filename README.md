# Datawarehouse Redshift
ETL pipeline for a datawarehouse (DWH) hosted on Redshift AWS

# Files

- AWS/ used to create aws infrastucture read more on section *Creating Infrastucture*
- sql_queries.py: contains all sql queries
- create_tables.py: Create all tables. Read more on details about the tables schema on section *Schemas*
- etl.py: Run ETL pipeline. The pipeline read data from s3 (schema format on subsection *Data in s3*), and converts the data to a Star Schema format (schema format on subsection *schema format on subsection *Datawarehouse Schema*)
- load_s3.py: Upload `*.json` data on folder `data/`to s3.
- dwg.cfg: Config file

# AWS Credentials
We are using aws as environment variables in this repo:
```bash
export AWS_ACCESS_KEY_ID=<YOUR_AWS_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<YOUR_AWS_SECRET_ACCESS_KEY>
export AWS_DEFAULT_REGION=<YOUR_REGION>
```

# Creating Infrastucture

## IAM
Create IAM roles for Redshift, run file: `iam.py`. It will create a role for Redshift with s3 Read access

## Redshift
Creates Redshift cluster and add Ingress rule for TCP connection on port 5439. Run file `redshift.py`

## S3
Upload all dataset to s3. Run `load_s3.py`

# Schemas

## Data in S3
This data will pass trough an ETL to be stored in Redshift
![Image](Images/stage_schema.png)
## Datawarehouse Schema
![Image](Images/star_schema.png)

# Usage

## Config file
Fill out the `dwh.cfg` with the desired redshift confirguration, s3 and IAM role

## Scripts
Once you have all the infraestructure ready:

- Run `create_tables.py`
- Run `etl.py`

# References

- https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
- https://www.flydata.com/blog/amazon-redshift-distkey-and-sortkey/
- https://www.intermix.io/blog/top-14-performance-tuning-techniques-for-amazon-redshift/
