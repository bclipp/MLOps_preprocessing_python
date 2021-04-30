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
    print(f"saving table to dbfs:/datalake/stocks_{uid}/data")
    try:
        df.write.format("delta").save(f"dbfs:/datalake/stocks_{uid}/data")
    except Exception as e:
        print(f"There was an error writing the delta stock table, : error:{e}")


if __name__ == "__main__":
    main()
