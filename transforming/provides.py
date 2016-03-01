from pyspark.sql import SQLContext
from pyspark.sql.types import *
from random import randint
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

provides_table = providesparts.map(lambda l : (l[0], l[4], l[9], processData(l[11].strip('"'))))

providesschemaString = 'hid hstate pid effective_score'
 
providesfields = [StructField(field_name, StringType(), True) for field_name in providesschemaString.split()]
providesfields[3].dataType = IntegerType()

providesschema = StructType(providesfields)
dfProvides = sqlContext.createDataFrame(provides_table, providesschema)
dfProvides.registerTempTable('provides_table')

# save files
provides_table.saveAsTextFile('/user/w205/hospital_compare/provides_table')
providesshemaResult = sc.parallelize(dfProvides)
providesshemaResult.saveAsTextFile('/user/w205/hospital_compare/surveys_schema')