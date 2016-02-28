#!/bin/bash
# My first script
echo "Exercise 1"

#make a dir in W205 for exercise 1 for preprocessing before putting into HDFS
mkdir exercise1
cd exercise1

# get the data zip file and unzip
wget https://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip
unzip  https://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip

# repalce all spaces with _ so that file names can be understood
for f in *\ *; do mv "$f" "${f// /_}"; done

# select a few files of interest and process them
tail -n +2 ./Hospital_General_Information.csv > ./hospitals.csv
tail -n +2 ./Timely_and_Effective_Care_-_Hospital.csv > ./effective_care.csv
tail -n +2 ./Readmissions_and_Deaths_-_Hospital.csv > ./readmissions.csv
tail -n +2 ./hvbp_hcahps_05_28_2015.csv > ./surveys_responses.csv
tail -n +2 ./Measure_Dates.csv > ./measures.csv
tail -n +2 ./Timely_and_Effective_Care_-_State.csv > ./effective_care_state.csv
tail -n +2 ./Readmissions_and_Deaths_-_State.csv > ./readmissions_state.csv

# delete any existing folder with the required name, and create a new one, put the files into HDFS
hdfs dfs -rm -r /user/w205/hospital_compare
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -put hospitals.csv /user/w205/hospital_compare
hdfs dfs -put readmissions.csv /user/w205/hospital_compare
hdfs dfs -put effective_care.csv  /user/w205/hospital_compare
hdfs dfs -put surveys_responses.csv /user/w205/hospital_compare
hdfs dfs -put measures.csv /user/w205/hospital_compare


