import os
from pprint import pprint
import configparser
import logging

import numpy as np
import pandas as pd
from botocore.exceptions import ClientError
import boto3
from sqlalchemy import Table, Column, Integer, Float, String

from helpers import timer
from scraper import scrape
from SQLManager import SQLManager

if not os.path.exists("logs"):
    os.mkdir("logs")

logging.basicConfig(
    filename="logs/app.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("app")

config = configparser.ConfigParser()
config.read("pipeline.conf")


@timer
def extract():
    """Run all process"""
    # Scraper
    try:
        books = scrape()
        logger.info("Page content scraped")
    except:
        logger.error("Page content could not be scraped")
        return False

    # csv file creation
    df = pd.DataFrame(books)
    filename = "books_latest.csv"
    filepath = f"files/{filename}"
    try:
        df.to_csv(filepath, index=False, na_rep="NA")
        logger.info("CSV file {} saved in files folder".format(filename))
    except FileNotFoundError:
        logger.error("File path does not exist")
        return False

    # Send to S3 bucket
    if send_to_s3(filename=filename, filepath=filepath, bucket="latest-books"):
        return True


@timer
def transform():
    """Get file from S3 bucket and clean it"""
    # Get file from S3
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket="latest-books", Key="books_latest.csv")
    df = pd.read_csv(obj["Body"])

    # Cleaning
    df["price"] = df["price"].astype("str")
    df["price"] = df["price"].str.replace("€", "")
    df["price"] = df["price"].str.strip()
    df["price"] = df["price"].replace("nan", np.nan)
    df["price"] = df["price"].str.replace(",", ".").astype(float)

    df["rating"] = df["rating"].astype("str")
    df["rating"] = df["rating"].str.replace(" étoiles sur 5", "")
    df["rating"] = df["rating"].str.replace(",", ".").fillna("0.0").astype(float)

    df["comments"] = (
        pd.to_numeric(df["comments"], errors="coerce").fillna(0.0).astype(int)
    )

    # Load in another S3 bucket (latest-books-cleaned)
    filename = "books_latest_cleaned.csv"
    filepath = f"files/{filename}"
    df.to_csv(filepath, index=False, na_rep="NA")
    if send_to_s3(filename=filename, filepath=filepath, bucket="latest-books-cleaned"):
        return True
    return False


def load():
    # Get file from S3 bucket latest-books-cleaned
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket="latest-books-cleaned", Key="books_latest_cleaned.csv")
    logger.info("Got csv file from latest-books-cleaned bucket")
    df = pd.read_csv(obj["Body"])

    # Get DB credentials and connection to DB
    db_name = config.get("postgres_credentials", "db_name")
    db_user = config.get("postgres_credentials", "db_user")
    db_password = config.get("postgres_credentials", "db_password")
    db_host = config.get("postgres_credentials", "db_host")
    db_port = config.get("postgres_credentials", "db_port")
    Manager = SQLManager(db_name, db_user, db_password, db_host, db_port)

    # Create table if not exists
    Table(
        "books",
        Manager.metadata,
        Column("id", Integer, primary_key=True, nullable=False),
        Column("title", String),
        Column("author", String),
        Column("format", String),
        Column("price", Float, nullable=True),
        Column("image", String, nullable=True),
        Column("rating", Float, nullable=True),
        Column("comments", Integer, nullable=True),
    )
    Manager.metadata.create_all(checkfirst=True)

    # Insert csv into table
    df.to_sql("books", Manager.engine, if_exists="replace", index=False)
    logger.info("Dataframe loaded to DB !")


def send_to_s3(filename, filepath, bucket):
    """Send file to S3 bucket"""
    s3 = boto3.client("s3")
    try:
        s3.upload_file(filepath, bucket, filename)
        logger.info("File {} sent to S3 bucket {}".format(filename, bucket))
    except ClientError as e:
        logger.ERROR(e)
        return False
    return True


def run_pipeline():
    if extract():
        if transform():
            load()


if __name__ == "__main__":
    run_pipeline()
