from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)

surveys = sc.textFile('/user/w205/hospital_compare/surveys_responses.csv')
filtered = surveys.filter(lambda x: "Not Available" not in x)

parts1 = filtered.map(lambda l: l.split(','))
parts = parts1.filter(lambda l: len(l) is 33)

surveys_table = parts.map(lambda l: ('hcahps', l[0], int(l[31].strip('"')), int(l[32].strip('"'))))

schemaString = 'sid hid base_score consistency_score'
 
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
fields[2].dataType = IntegerType()
fields[3].dataType = IntegerType()

schema = StructType(fields)
schemasurveys = sqlContext.createDataFrame(surveys_table, schema)
schemasurveys.registerTempTable('surveys_table')

# save files
surveys_table.saveAsTextFile('/user/w205/hospital_compare/surveys_table')
shemaResult = sc.parallelize(schemasurveys)
shemaResult.saveAsTextFile('/user/w205/hospital_compare/surveys_schema')
