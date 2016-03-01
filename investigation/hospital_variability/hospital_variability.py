from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
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


procedures = sc.textFile('/user/w205/hospital_compare/measures.csv')
filtered = procedures.filter(lambda x: "Not Available" not in x)
parts = filtered.map(lambda l: l.split(','))

procedures_table = parts.map(lambda p: (p[0], p[1]))
schemaString = 'pname pid'
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schemaProcedures = sqlContext.createDataFrame(procedures_table, schema)
schemaProcedures.registerTempTable('procedures_table')

procedure_join_provides = 'SELECT provides_table.pid, procedures_table.pname, AVG(provides_table.effective_score*provides_table.effective_score) - AVG(provides_table.effective_score)*AVG(provides_table.effective_score) AS variance FROM provides_table INNER JOIN procedures_table ON provides_table.pid = procedures_table.pid GROUP BY provides_table.pid, procedures_table.pname ORDER BY variance DESC LIMIT 10'

procedureVar_result = sqlContext.sql(procedure_join_provides)
procedureVar_result.show()
