from datetime import datetime

from delta.tables import *
import yfinance as yf
import uuid

from pyspark.sql import SparkSession
import sys


def main():
    spark = SparkSession \
        .builder \
        .appName("PythonPageRank") \
        .getOrCreate()
    uid = sys.argv[1]
    spark.conf.set("spark.sql.execution.arrow.enabled", True)
    pdf = yf.download(tickers='UBER', period='10y', interval='1d')
    pdf["AdjClose"] = pdf["Adj Close"]
    pdf = pdf.drop("Adj Close", axis=1)
    df = spark.createDataFrame(pdf)
    print(f"dbfs:/datalake/stocks_{uid}/data")
    df.write.format("delta").save(f"dbfs:/datalake/stocks_{uid}/data")


if __name__ == "__main__":
    main()
