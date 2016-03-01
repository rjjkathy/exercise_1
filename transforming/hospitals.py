from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)
lines = sc.textFile('/user/w205/hospital_compare/hospitals.csv')

filtered = lines.filter(lambda x: "Not Available" not in x)
parts = filtered.map(lambda l: l.split(','))
hospitals_table = parts.map(lambda p: (p[0], p[1], p[4]))

schemaString = 'hid hname hstate'
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schemaHospitals = sqlContext.createDataFrame(hospitals_table, schema)

schemaHospitals.registerTempTable('hospitals_table')

# save files
hospitals_table.saveAsTextFile('/user/w205/hospital_compare/hospitals_table')
shemaResult = sc.parallelize(schemaHospitals)
shemaResult.saveAsTextFile('/user/w205/hospital_compare/hospitals_schema')


