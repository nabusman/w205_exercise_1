from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


save_file_path = "file:///home/w205/exercise1/transformed_data/"

schema_list = ['provided_id', 'hospital_name', 'address', 'city', 'state', 'zip_code', 'country', 'phone_number', 'hospital_type', 'hospital_ownership', 'emergency_services']
hospitalsRDD = sc.textFile("hdfs:///user/w205/hospital_compare/hospital/hospitals-headless.csv").map(lambda x: x.split(',')).filter(lambda x: len(x) == len(schema_list))
fields = [StructField(field_name, StringType(), True) for field_name in schema_list]
schema = StructType(fields)
hospitalsDF = sqlContext.createDataFrame(hospitalsRDD, schema)
hospitalsDF.save(save_file_path + "hospitals.parquet")
hospitalsDF.rdd.saveAsTextFile(save_file_path + "hospitals.txt")

schema_list = ['provider_id', 'hospital_name', 'address', 'city', 'state', 'zip_code', 'county_name', 'phone_number', 'condition', 'measure_id', 'measure_name', 'score', 'sample', 'footnote', 'measure_start_date', 'measure_end_date']
effective_careRDD = sc.textFile("hdfs:///user/w205/hospital_compare/effective_care/effective_care-headless.csv").map(lambda x: x.split(',')).filter(lambda x: len(x) == len(schema_list))
fields = [StructField(field_name, StringType(), True) for field_name in schema_list]
schema = StructType(fields)
effective_careDF = sqlContext.createDataFrame(effective_careRDD, schema)
effective_careDF.save(save_file_path + "effective_care.parquet")
effective_careDF.rdd.saveAsTextFile(save_file_path + "effective_care.txt")

# schema_list = ['measure_name', 'measure_id', 'measure_start_quarter', 'measure_start_date', 'measure_end_quarter', 'measure_end_date']
# measuresRDD = sc.textFile("hdfs:///user/w205/hospital_compare/measures/measures-headless.csv").map(lambda x: x.split(',')).filter(lambda x: len(x) == len(schema_list))
# fields = [StructField(field_name, StringType(), True) for field_name in schema_list]
# schema = StructType(fields)
# measuresDF = sqlContext.createDataFrame(measuresRDD, schema)


schema_list = ['provider_id', 'hospital_name', 'address', 'city', 'state', 'zip_code', 'county_name', 'phone_number', 'measure_name', 'measure_id', 'compared_to_national', 'denominator', 'score', 'lower_estimate', 'higher_estimate', 'footnote', 'measure_start_date', 'measure_end_date']
readmissionRDD = sc.textFile("hdfs:///user/w205/hospital_compare/readmissions/readmissions-headless.csv").map(lambda x: x.split(',')).filter(lambda x: len(x) == len(schema_list))
fields = [StructField(field_name, StringType(), True) for field_name in schema_list]
schema = StructType(fields)
readmissionDF = sqlContext.createDataFrame(readmissionRDD, schema)
readmissionDF.save(save_file_path + "readmissions.parquet")
readmissionDF.rdd.saveAsTextFile(save_file_path + "readmissions.txt")

schema_list = ['provider_number', 'hospital_name', 'address', 'city', 'state', 'zip_code', 'county_name', 'communication_with_nurses_achievement_points', 'communication_with_nurses_improvement_points', 'communication_with_nurses_dimension_score', 'communication_with_doctors_achievement_points', 'communication_with_doctors_improvement_points', 'communication_with_doctors_dimension_score', 'responsiveness_of_hospital_staff_achievement_points', 'responsiveness_of_hospital_staff_improvement_points', 'responsiveness_of_hospital_staff_dimension_score', 'pain_management_achievement_points', 'pain_management_improvement_points', 'pain_management_dimension_score', 'communication_about_medicines_achievement_points', 'communication_about_medicines_improvement_points', 'communication_about_medicines_dimension_score', 'cleanliness_and_quietness_of_hospital_environment_achievement_points', 'cleanliness_and_quietness_of_hospital_environment_improvement_points', 'cleanliness_and_quietness_of_hospital_environment_dimension_score', 'discharge_information_achievement_points', 'discharge_information_improvement_points', 'discharge_information_dimension_score', 'overall_rating_of_hospital_achievement_points', 'overall_rating_of_hospital_improvement_points', 'overall_rating_of_hospital_dimension_score', 'hcahps_base_score', 'hcahps_consistency_score']
surveyRDD = sc.textFile("hdfs:///user/w205/hospital_compare/survey/survey_responses-headless.csv").map(lambda x: x.split(',')).filter(lambda x: len(x) == len(schema_list))
fields = [StructField(field_name, StringType(), True) for field_name in schema_list]
schema = StructType(fields)
surveyDF = sqlContext.createDataFrame(surveyRDD, schema)
surveyDF.save(save_file_path + "survey.parquet")
surveyDF.rdd.saveAsTextFile(save_file_path + "survey.txt")