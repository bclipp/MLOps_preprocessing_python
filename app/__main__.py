from delta.tables import *
import yfinance as yf
# import dbutils
import uuid
import datetime
from pyspark.sql import SparkSession


def main():
    spark = SparkSession \
        .builder \
        .appName("PythonPageRank") \
        .getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", True)
    pdf = yf.download(tickers='UBER', period='10y', interval='1d')
    pdf["AdjClose"] = pdf["Adj Close"]
    pdf = pdf.drop("Adj Close", axis=1)
    now = datetime.now()
    timestamp = now.strftime("%m%d%Y%H%M")
    uid = str(uuid.uuid1()).replace('-', '')
#    dbutils.fs.mkdirs("/dbfs/datalake/dat")
    df = spark.createDataFrame(pdf)
    df.write.format("delta").save(f"/dbfs/datalake/stocks_{uid}_{timestamp}/data")


if __name__ == "__main__":
    main()
