from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as func

schema_list = ['provider_id', 'hospital_name', 'address', 'city', 'state', 'zip_code', 'county_name', 'phone_number', 'condition', 'measure_id', 'measure_name', 'score', 'sample', 'footnote', 'measure_start_date', 'measure_end_date']
effective_careRDD = sc.textFile("hdfs:///user/w205/hospital_compare/effective_care/effective_care-headless.csv").map(lambda x: x.replace('"','').split(',')).filter(lambda x: len(x) == len(schema_list))
fields = [StructField(field_name, StringType(), True) for field_name in schema_list]
schema = StructType(fields)
effective_careDF = sqlContext.createDataFrame(effective_careRDD, schema)

measures = effective_careDF.select("measure_name").distinct().map(lambda x: x[0]).collect()

for measure in measures:
  print(measure)


hospital_metrics = effective_careDF.filter(effective_careDF["score"].rlike("\d+")).select(effective_careDF["provider_id"], effective_careDF["score"].cast("integer").alias("score_int"), effective_careDF["sample"].cast("integer").alias("sample_int"))
hospital_metrics = hospital_metrics.groupBy("provider_id").agg(func.avg(hospital_metrics["score_int"]).alias("average_score"), 
  func.max(hospital_metrics["score_int"]).alias("max_score"), 
  func.min(hospital_metrics["score_int"]).alias("min_score"),
  func.sum(hospital_metrics["sample_int"]).alias("total_samples"))
hospital_metrics = hospital_metrics.filter(hospital_metrics["total_samples"] > 30).withColumn('range', hospital_metrics["max_score"] - hospital_metrics["min_score"])
hospital_metrics = hospital_metrics.withColumn('effectiveness', hospital_metrics["average_score"] / hospital_metrics["range"])

hospital_metrics.orderBy(hospital_metrics["range"].desc()).limit(10).show()
hospital_metrics.orderBy(hospital_metrics["range"]).limit(10).show()