# Weather Result

## Instructions
Convert the weather data into parquet format. Set the raw group to appropriate value you see fit for this data. The converted data should be queryable to answer the following question.
- Which date was the hottest day?
- What was the temperature on that day? 
- In which region was the hottest day?

## Assumptions 
Records having ScreenTemperature values = "-99" are not relevant and are not a limiting factor for the calculation of the hottest temperature. 

## How to run

If you have PySpark pip installed into your environment (e.g., pip install pyspark), you can run your application with the regular Python interpreter or use the provided ‘spark-submit’ as you prefer by downloading and copying "weather_result-master" in YOUR_SPARK_HOME/bin/

    # Use the Python interpreter to run your application from weather_result-master
    $ python weather_result.py
    

## Tests 

```python
from pyspark.sql import SparkSession
```


```python
spark = SparkSession \
    .builder \
    .appName("Weather Report") \
    .getOrCreate()
```


```python
df = spark.read.format("csv")\
        .options(header="true", inferschema="true")\
        .load("Data Engineer Test_Green Flag/weather.*.csv")
df.show(1)
```

    +----------------+---------------+-------------------+-------------+---------+--------+----------+-----------------+--------+----------------------+-----------------+--------+---------+-----------------+--------+
    |ForecastSiteCode|ObservationTime|    ObservationDate|WindDirection|WindSpeed|WindGust|Visibility|ScreenTemperature|Pressure|SignificantWeatherCode|         SiteName|Latitude|Longitude|           Region| Country|
    +----------------+---------------+-------------------+-------------+---------+--------+----------+-----------------+--------+----------------------+-----------------+--------+---------+-----------------+--------+
    |            3002|              0|2016-02-01 00:00:00|           12|        8|    null|     30000|              2.1|     997|                     8|BALTASOUND (3002)|  60.749|   -0.854|Orkney & Shetland|SCOTLAND|
    +----------------+---------------+-------------------+-------------+---------+--------+----------+-----------------+--------+----------------------+-----------------+--------+---------+-----------------+--------+
    only showing top 1 row
    



```python
#Expecting 194,697 rows
df.count()
```




    194697




```python
df.printSchema()
```

    root
     |-- ForecastSiteCode: integer (nullable = true)
     |-- ObservationTime: integer (nullable = true)
     |-- ObservationDate: timestamp (nullable = true)
     |-- WindDirection: integer (nullable = true)
     |-- WindSpeed: integer (nullable = true)
     |-- WindGust: integer (nullable = true)
     |-- Visibility: integer (nullable = true)
     |-- ScreenTemperature: double (nullable = true)
     |-- Pressure: integer (nullable = true)
     |-- SignificantWeatherCode: integer (nullable = true)
     |-- SiteName: string (nullable = true)
     |-- Latitude: double (nullable = true)
     |-- Longitude: double (nullable = true)
     |-- Region: string (nullable = true)
     |-- Country: string (nullable = true)
    



```python
# DataFrames can be saved as Parquet files, maintaining the schema information.
df.write.mode("overwrite").parquet("weather.parquet")

# Read in the Parquet file created above.
# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
parquetFile = spark.read.parquet("weather.parquet")

#Schema sense check
parquetFile.printSchema()

parquetFile.describe("ScreenTemperature").show()


```

    root
     |-- ForecastSiteCode: integer (nullable = true)
     |-- ObservationTime: integer (nullable = true)
     |-- ObservationDate: timestamp (nullable = true)
     |-- WindDirection: integer (nullable = true)
     |-- WindSpeed: integer (nullable = true)
     |-- WindGust: integer (nullable = true)
     |-- Visibility: integer (nullable = true)
     |-- ScreenTemperature: double (nullable = true)
     |-- Pressure: integer (nullable = true)
     |-- SignificantWeatherCode: integer (nullable = true)
     |-- SiteName: string (nullable = true)
     |-- Latitude: double (nullable = true)
     |-- Longitude: double (nullable = true)
     |-- Region: string (nullable = true)
     |-- Country: string (nullable = true)
    
    +-------+------------------+
    |summary| ScreenTemperature|
    +-------+------------------+
    |  count|            194697|
    |   mean| 3.332367216752207|
    | stddev|13.349063350594122|
    |    min|             -99.0|
    |    max|              15.8|
    +-------+------------------+
    



```python
# Parquet file temporary view and fed into SQL statement.
parquetFile.createOrReplaceTempView("weatherparquetFile")

hottestTemperature = spark.sql("SELECT max(ScreenTemperature) FROM weatherparquetFile")

hottestTemperature.show()
                               

```

    +----------------------+
    |max(ScreenTemperature)|
    +----------------------+
    |                  15.8|
    +----------------------+
    



```python
result_sql = spark.sql("SELECT ScreenTemperature, ObservationDate, Region FROM weatherparquetFile WHERE ScreenTemperature = (SELECT max(ScreenTemperature) from weatherparquetFile) " )
result_sql.show()
```

    +-----------------+-------------------+--------------------+
    |ScreenTemperature|    ObservationDate|              Region|
    +-----------------+-------------------+--------------------+
    |             15.8|2016-03-17 00:00:00|Highland & Eilean...|
    +-----------------+-------------------+--------------------+
    



```python
import pyspark.sql.functions as f
```


```python
#Max_ScreenTemperature = df.agg({"ScreenTemperature": "max"}).collect()[0][0]

Max_ScreenTemperature = parquetFile.groupBy().agg(f.max(parquetFile.ScreenTemperature)).collect()[0][0]
```


```python
print(Max_ScreenTemperature)
```

    15.8



```python
#using Max_ScreenTemperature to filter my dataframe
result_pF = parquetFile.filter(parquetFile.ScreenTemperature == Max_ScreenTemperature).select(["ScreenTemperature", "ObservationDate", "Region"])
result_pF.show(1, False)
```

    +-----------------+-------------------+----------------------+
    |ScreenTemperature|ObservationDate    |Region                |
    +-----------------+-------------------+----------------------+
    |15.8             |2016-03-17 00:00:00|Highland & Eilean Siar|
    +-----------------+-------------------+----------------------+
    



```python
result_pF.collect()[0][1].strftime("%Y-%m-%d")
```




    '2016-03-17'




```python
result_row = result_sql.collect()[0]

result_dict = result_row.asDict()

```


```python
print("Hottest Day Data")
print ("----------------")
for k,v in result_dict.items():
    print(k,"=",v)
```

    Hottest Day Data
    ----------------
    ScreenTemperature = 15.8
    ObservationDate = 2016-03-17 00:00:00
    Region = Highland & Eilean Siar



```python
result_row = result_sql.collect()[0]
```


```python
result_dict = result_row.asDict()

print("Hottest Day Data")
print ("----------------")
for k,v in result_dict.items():
    print(k,"=",v)
```

    Hottest Day Data
    ----------------
    ScreenTemperature = 15.8
    ObservationDate = 2016-03-17 00:00:00
    Region = Highland & Eilean Siar



```python

```

