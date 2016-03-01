import os
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext
sc = SparkContext("local", "Simple App")

print "I will call this other program called hello.py"
os.system("python /user/w205/hospital_compare/providers.py")
os.system("python surveys.py")
os.system("python procedures.py")
os.system("python hospitals.py")


procedure_join_survey = 'SELECT provides_table.hid, provides_table.hname, provides_table.hstate, AVG(provides_table.effective_score + surveys_table.base_score + surveys_table.consistency_score) AS final_score FROM provides_table INNER JOIN surveys_table ON provides_table.hid = surveys_table.hid GROUP BY provides_table.hid, provides_table.hname, provides_table.hstate ORDER BY final_score DESC LIMIT 10'

hospital_result = sqlContext.sql(procedure_join_survey)
hospital_result.show()
