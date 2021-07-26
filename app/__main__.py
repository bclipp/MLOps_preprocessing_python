"""
This module is dedicated to preprocessing data for ML training
"""

import sys
# from datetime import datetime

from delta.tables import *
import yfinance as yf

from pyspark.sql import SparkSession


def main():
    """
    This is the entry point for the preprocessing
    """
    spark = SparkSession \
        .builder \
        .appName("PythonPageRank") \
        .getOrCreate()
    uid = sys.argv[1]
    spark.conf.set("spark.sql.execution.arrow.enabled", True)
    pdf = yf.download(tickers='UBER', period='10y', interval='1d')
    pdf["AdjClose"] = pdf["Adj Close"]
    pdf = pdf.drop("Adj Close", axis=1)
    data_frame = spark.createDataFrame(pdf)
    print(f"saving table to dbfs:/datalake/stocks_{uid}/data")
    try:
        data_frame.write.format("delta").save(f"dbfs:/datalake/stocks_{uid}/data")
    except Exception as error:
        print(f"There was an error writing the delta stock table, : error:{error}")


if __name__ == "__main__":
    main()
