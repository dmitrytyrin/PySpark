from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *


def pp(s):
    decorator = "=" * 50
    print("\n\n{} {} {}".format(decorator, s, decorator))


if __name__ == "__main__":

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

    crimeFacts = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("boston_crimes/crime.csv")

    pp('1.crimes_total')
    crimeFacts.createOrReplaceTempView("crimes")
    spark.sql("select DISTRICT, count(*) as count_crimes from crimes group by DISTRICT order by DISTRICT").show()

    pp('2.crimes_monthly')
    df = crimeFacts.select('DISTRICT', 'YEAR', 'MONTH')
    df = df\
        .withColumn('temp', f.concat(f.col('DISTRICT'), f.lit('_'), f.col('YEAR'), f.lit('_'), f.col('MONTH')))\
        .select('DISTRICT', 'temp')\
        .orderBy('DISTRICT', ascending=False)\
        .groupBy('DISTRICT', 'temp')\
        .agg(count('temp').alias('c'))\
        .select('DISTRICT', 'c')\
        .groupBy('DISTRICT') \
        .agg(mean('c').alias('mean_month'))
    df.show()

    pp('3.frequent_crime_types')

    offenseCodes = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("boston_crimes/offense_codes.csv")

    df = crimeFacts\
        .select('DISTRICT', 'OFFENSE_CODE')\
        .join(broadcast(offenseCodes), offenseCodes.CODE == crimeFacts.OFFENSE_CODE) \
        .withColumn('crime_type', split('NAME', '-').getItem(0))\
        .select('DISTRICT', 'crime_type')\
        .withColumn('temp', f.concat(f.col('DISTRICT'), f.lit('_'), f.col('crime_type')))\
        .orderBy('DISTRICT', ascending=False)\
        .groupBy('DISTRICT', 'temp')\
        .agg(count('temp').alias('c'))\
        .orderBy('c', ascending=False) \
        .withColumn('crime_type', split('temp', '_').getItem(1)) \
        .select('DISTRICT', 'crime_type', 'c')\
        #.show(5)

    w = Window.partitionBy(df.DISTRICT).orderBy(df.c.desc())
    df1 = df\
        .withColumn('row', row_number().over(w))\
        .where(col('row') <= 3).drop('row', 'c')\
        .groupby('DISTRICT')\
        .agg(concat_ws(", ", collect_list(col('crime_type'))).alias('crimes'))\
        .show()

    pp('4.lat')
    df = crimeFacts.select('DISTRICT', 'Lat')
    df = df\
        .groupby('DISTRICT')\
        .agg(avg('Lat'))\
        .show()

    pp('5.lng')
    df = crimeFacts.select('DISTRICT', 'Long')
    df = df\
        .groupby('DISTRICT')\
        .agg(avg('Long'))\
        .show()

    spark.stop()
