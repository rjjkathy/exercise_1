from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext
sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)

provides = sc.textFile('/user/w205/hospital_compare/effective_care.csv')
providesfiltered = provides.filter(lambda x: ("Not Available" not in x ) )

providesparts1 = providesfiltered.map(lambda l: l.split(','))
providesparts = providesparts1.filter(lambda l: len(l) is 16)

def processData(x): 
    if "High (40,000 - 59,999 patients annually)" in x:
        value = 60000
    elif "Medium (20,000 - 39,999 patients annually" in x:
        value = randint(20000,39999)
    elif "Low (0 - 19,999 patients annually)" in x:
        value = randint(0, 19999)
    else:    
        value = int(x)
        return value

provides_table = providesparts.map(lambda l : (l[0], l[1], l[4], l[9], processData(l[11].strip('"'))))

providesschemaString = 'hid hname hstate pid effective_score'
 
providesfields = [StructField(field_name, StringType(), True) for field_name in providesschemaString.split()]
providesfields[4].dataType = IntegerType()

providesschema = StructType(providesfields)
dfProvides = sqlContext.createDataFrame(provides_table, providesschema)
dfProvides.registerTempTable('provides_table')


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


procedure_join_survey = 'SELECT provides_table.hstate, AVG(provides_table.effective_score + surveys_table.base_score + surveys_table.consistency_score) AS final_score FROM provides_table INNER JOIN surveys_table ON provides_table.hid = surveys_table.hid GROUP BY provides_table.hstate ORDER BY final_score DESC LIMIT 10'

state_result = sqlContext.sql(procedure_join_survey)
state_result.show()
