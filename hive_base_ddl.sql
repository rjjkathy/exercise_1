/* 1. hospitals.csv */
DROP TABLE hospitals;
CREATE EXTERNAL TABLE hospitals(
hid string,
hname string,
haddress string,
city string,
hstate string,
zip string,
county string,
phone string,
htype string,
hownership string,
emergency string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
"separatorChar" = ',',
"quoteChar" = '"',
"escapeChar" = '\\' 
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare';

/* 2. effective_care.csv */
DROP TABLE effective_care;
CREATE EXTERNAL TABLE effective_care(
hid string,
hname string,
haddress string,
city string,
hstate string,
zip string,
county string,
phone string,
condition string,
pid string,
pname string,
score int,
sample string,
footnote string,
measureStart datetime,
measureEnd datetime
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
"separatorChar" = ',',
"quoteChar" = '"',
"escapeChar" = '\\' 
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare';

/* 3. survey_responses */
DROP TABLE survey_responses;
CREATE EXTERNAL TABLE survey_responses(
hid string,
hname string,
haddress string,
city string,
hstate string,
zip string,
county string,
nurse_achievment string,
nurse_improvement string,
nurse_dimension string,
doctor_achievment string,
doctor_improvement string,
doctor_dimension string,
staff_achievment string,
staff_improvement string,
staff_dimension string,
management_achievment string,
management_improvement string,
management_dimension string,
medicine_achievment string,
medicine_improvement string,
medicine_dimension string,
dischargeInfo_achievment string,
dischargeInfo_improvement string,
dischargeInfo_dimension string,
overall_achievment string,
overall_improvement string,
overall_dimension string,
base_score int,
consistency_score int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
"separatorChar" = ',',
"quoteChar" = '"',
"escapeChar" = '\\' 
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare';

/* 4. measures */
DROP TABLE measures;
CREATE EXTERNAL TABLE measures(
pname string,
pid string,
measureStartQ string,
measureStart datetime,
measureEndQ string,
measureEnd datetime
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
"separatorChar" = ',',
"quoteChar" = '"',
"escapeChar" = '\\' 
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare';

/* 5. readmission */
DROP TABLE readmissions;
CREATE EXTERNAL TABLE readmissions(
hid string,
hname string,
haddress string,
city string,
hstate string,
zip string,
county string,
phone string,
pname string,
pid string,
compareToNational string,
denominator string,
score float,
lower_estimate string,
higher_esitmate string,
footnote string,
measureStart datetime,
measureEnd datetime
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
"separatorChar" = ',',
"quoteChar" = '"',
"escapeChar" = '\\' 
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare';

