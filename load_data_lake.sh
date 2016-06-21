#!/bin/bash

mkdir raw_data
cd raw_data
wget https://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip
unzip Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s\?content_type\=application%2Fzip\;\ charset\=binary
cd ..
cp raw_data/Hospital\ General\ Information.csv hospitals.csv
cp raw_data/Timely\ and\ Effective\ Care\ -\ Hospital.csv effective_care.csv
cp raw_data/Readmissions\ and\ Deaths\ -\ Hospital.csv readmissions.csv
cp raw_data/Measure\ Dates.csv measures.csv
cp raw_data/hvbp_hcahps_05_28_2015.csv survey_responses.csv

tail -n +2 hospitals.csv > hospitals-headless.csv
tail -n +2 effective_care.csv > effective_care-headless.csv
tail -n +2 readmissions.csv > readmissions-headless.csv
tail -n +2 measures.csv > measures-headless.csv
tail -n +2 survey_responses.csv > survey_responses-headless.csv

hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -put hospitals-headless.csv /user/w205/hospital_compare
hdfs dfs -put effective_care-headless.csv /user/w205/hospital_compare
hdfs dfs -put readmissions-headless.csv /user/w205/hospital_compare
hdfs dfs -put measures-headless.csv /user/w205/hospital_compare
hdfs dfs -put survey_responses-headless.csv /user/w205/hospital_compare

