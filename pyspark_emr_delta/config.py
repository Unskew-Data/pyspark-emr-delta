from typing import Callable
import os


class Config:

    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "DEVELOPMENT")

    S3_BUCKET: str = os.getenv("S3_BUCKET", "sales-data-007")
    BASE_PATH = f"s3a://{S3_BUCKET}"
    DATA_PATH: str = os.path.join(BASE_PATH, "sales.csv")
    OUT_PATH: str = os.path.join(BASE_PATH, "out")

    APP_NAME = "pyspark_emr_delta"


config = Config()
