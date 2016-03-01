from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)

surveys = sc.textFile('/user/w205/hospital_compare/surveys_responses.csv')
surveyfiltered = surveys.filter(lambda x: "Not Available" not in x)

surveyparts1 = surveyfiltered.map(lambda l: l.split(','))
surveyparts = surveyparts1.filter(lambda l: len(l) is 33)

surveys_table = surveyparts.map(lambda l: ('hcahps', l[0], int(l[31].strip('"')), int(l[32].strip('"'))))

schemaString = 'sid hid base_score consistency_score'
 
surveyfields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
surveyfields[2].dataType = IntegerType()
surveyfields[3].dataType = IntegerType()

surveyschema = StructType(surveyfields)
schemasurveys = sqlContext.createDataFrame(surveys_table, surveyschema)
schemasurveys.registerTempTable('surveys_table')




# save files
surveys_table.saveAsTextFile('/user/w205/hospital_compare/surveys_table')
shemaResult = sc.parallelize(schemasurveys)
shemaResult.saveAsTextFile('/user/w205/hospital_compare/surveys_schema')
