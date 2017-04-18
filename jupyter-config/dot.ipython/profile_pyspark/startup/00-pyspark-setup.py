import os
import sys

#
# /home/$USER/spark-dependencies/
#	|-- jars
#	|	|-- abc.jar
#	|	|-- xyz.jar
#	|-- packages.txt
#
#
# packages.txt
#   com.databricks:spark-avro_2.11:3.2.0
#   ml.combust.mleap:mleap-spark_2.11:0.6.0
#

dependencyBase = "/home/%s/spark-dependencies" % os.environ["USER"]
jarBase = "%s/jars" % dependencyBase

jars = ",".join(["%s/%s" % (jarBase, jar) for jar in os.listdir(jarBase) if jar != "packages.txt"])

with open("%s/packages.txt" % dependencyBase, "r") as fd:
	packages = [line for line in fd.read().split("\n") if line[0] != "#"]

submitArgs = " --master yarn --deploy-mode client" + \
             " --num-executors 6 --executor-memory 4GB" + \
             " --conf spark.pyspark.python=python3"

if len(packages) > 0:
	submitArgs += " --packages %s" % ",".join(packages)

if len(jars) > 0:
	submitArgs += " --jars %s" % jars

submitArgs += " pyspark-shell"

# Set Spark to HDP Spark 2 
spark_home = "/usr/hdp/current/spark2-client"
os.environ["SPARK_HOME"] = spark_home

os.environ["PYSPARK_SUBMIT_ARGS"] = submitArgs

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

files = os.listdir(os.path.join(spark_home, "python/lib"))
py4j = [f for f in files if f[:4] == "py4j"][0]
sys.path.insert(0, spark_home + "/python")
sys.path.insert(0, os.path.join(spark_home, 'python/lib', py4j))

# Run Spark Python Shell
filename = os.path.join(spark_home, 'python/pyspark/shell.py')
exec(compile(open(filename, "rb").read(), filename, 'exec'))
