from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext
sc = SparkContext("local", "Simple App")

sqlContext = SQLContext(sc)

procedures = sc.textFile('/user/w205/hospital_compare/measures.csv')
filtered = procedures.filter(lambda x: "Not Available" not in x)
parts = filtered.map(lambda l: l.split(','))

procedures_table = parts.map(lambda p: (p[0], p[1]))
schemaString = 'pname pid'
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schemaProcedures = sqlContext.createDataFrame(procedures_table, schema)
schemaProcedures.registerTempTable('procedures_table')

# save files
procedures_table.saveAsTextFile('/user/w205/hospital_compare/procedures_table')
shemaResult = sc.parallelize(schemaProcedures)
shemaResult.saveAsTextFile('/user/w205/hospital_compare/procedures_schema')