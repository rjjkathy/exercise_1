from pyspark.sql import SQLContext
from pyspark.sql.types import *
from random import randint

sqlContext = SQLContext(sc)

provides = sc.textFile('/user/w205/hospital_compare/effective_care.csv')
filtered = provides.filter(lambda x: ("Not Available" not in x ) )

parts1 = filtered.map(lambda l: l.split(','))
parts = parts1.filter(lambda l: len(l) is 16)
parts.count()

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

provides_table = parts.map(lambda l : (l[0], l[4], l[9], processData(l[11].strip('"'))))

schemaString = 'hid hstate pid effective_score'
 
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
fields[3].dataType = IntegerType()

schema = StructType(fields)
schemaProvides = sqlContext.createDataFrame(provides_table, schema)
schemaProvides.registerTempTable('provides_table')

# save files
provides_table.saveAsTextFile('/user/w205/hospital_compare/provides_table')
shemaResult = sc.parallelize(schemaProvides)
shemaResult.saveAsTextFile('/user/w205/hospital_compare/provides_schema')