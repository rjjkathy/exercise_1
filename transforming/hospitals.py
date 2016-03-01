from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext
sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)

hospitals = sc.textFile('/user/w205/hospital_compare/hospitals.csv')

hospitalsfiltered = hospitals.filter(lambda x: "Not Available" not in x)
hospitalsparts = hospitalsfiltered.map(lambda l: l.split(','))
hospitals_table = hospitalsparts.map(lambda p: (p[0], p[1], p[4]))

hospitalsschemaString = 'hid hname hstate'
hospitalsfields = [StructField(field_name, StringType(), True) for field_name in hospitalsschemaString.split()]
hospitalsschema = StructType(hospitalsfields)
schemaHospitals = sqlContext.createDataFrame(hospitals_table, hospitalsschema)

schemaHospitals.registerTempTable('hospitals_table')

# save files
hospitals_table.saveAsTextFile('/user/w205/hospital_compare/hospitals_table')
shemaResult = sc.parallelize(schemaHospitals)
shemaResult.saveAsTextFile('/user/w205/hospital_compare/hospitals_schema')


