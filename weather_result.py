from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import Row




def process_parquet_file (spark,csv_filepath,parquet_filepath):
    #open csv file and load into a Dataframe
    df = spark.read.format("csv")\
            .options(header="true", inferschema="true")\
            .load(csv_filepath)

    #Dataframe saved as Parquet file maintaining schema formation.
    df.write.mode("overwrite").parquet(parquet_filepath)

    #return Dataframe in parquet format
    return spark.read.parquet(parquet_filepath)

def weather_result(spark, csv_filepath, parquet_filepath):

    parquetFile = process_parquet_file(spark,csv_filepath, parquet_filepath)

    Max_ScreenTemperature = parquetFile.groupBy().agg(f.max(parquetFile.ScreenTemperature)).collect()[0][0]

    result = parquetFile.filter(parquetFile.ScreenTemperature == Max_ScreenTemperature)\
             .select(["ScreenTemperature", "ObservationDate", "Region"])

    # Alternatively
    # Parquet file temporary view and fed into SQL statement.
    # parquetFile.createOrReplaceTempView("weatherparquetFile")
    # result = spark.sql("SELECT ScreenTemperature, ObservationDate, Region FROM weatherparquetFile WHERE ScreenTemperature =
    #			\(SELECT max(ScreenTemperature) from weatherparquetFile) " )

    result_row = result.collect()[0]

    result_dict = result_row.asDict()

    print("\r\r")
    print("Hottest Day Data")
    print ("*****************")
    for k,v in result_dict.items():
        print(k,"=",v)
    print("\r\r")


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Weather result")\
        .getOrCreate()

    weather_result(spark,csv_filepath="./data/weather.*.csv", parquet_filepath = "./data/weather.parquet")

    spark.stop()
