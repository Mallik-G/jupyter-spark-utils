# Copyright 2017 Bernhard Walter
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function
import sys

def getLibs(sc, conf):
    key = ""
    if sc.version[0] == "2":
        if conf.get('spark.master').startswith("yarn"):
            key = 'spark.yarn.dist.jars'
        elif conf.get("spark.master").startswith("local"):
            key = 'spark.jars'
    elif sc.version[0:3] == "1.6":
        key = 'spark.jars'

    if key != "":
        return [p.split("/")[-1] for p in conf.get(key).split(",")]
    else:
        println("Configuration not supported")
        return []

def extraLibs(sc, conf):
    excludes = [
        "com.chuusai_shapeless", "com.github.fommil", "com.github.rwl_",
        "com.google.protobuf", "com.jsuereth_scala", "com.lihaoyi",
        "com.thoughtworks.paranamer", "com.trueaccord", "com.trueaccord",
        "com.typesafe", "io.spray", "junit_junit", "net.sf.opencsv",
        "net.sourceforge.f2j", "org.apache.commons", "org.apache.spark",
        "org.codehaus.jackson", "org.hamcrest", "org.scala", "org.scala-lang",
        "org.scalanlp", "org.scalatest", "org.slf4j", "org.spark-project",
        "org.spire-math_spire", "org.tukaani", "org.xerial.snappy", "py4j",
        "pyspark.zip"
    ]
    libs = []
    for lib in getLibs(sc, conf):
        for ex in excludes:
            if lib.startswith(ex):
                lib = None
                break
        if lib is not None:
            libs.append(lib)
    return sorted(libs)


def overview():
    sparkCtx = sys.modules["pyspark"].SparkContext.getOrCreate()
    # sparkConf = sparkCtx.getConf()
    if sparkCtx.version[0] == "2":
        sparkConf = sparkCtx.getConf()
    elif sparkCtx.version[0:3] == "1.6":
        sparkConf = sparkCtx._conf
    else:
         println("Configuration not supported")

    print("Python:", sys.version.split("\n")[0])
    print("")
    print("Spark:  %s (master: %s, executors: %s, executor memory: %s)" % (sparkCtx.version,
                                                                           sparkConf.get("spark.master"),
                                                                           sparkConf.get("spark.executor.instances"),
                                                                           sparkConf.get("spark.executor.memory")))
    print("")
    print("Spark Extra Libs:")
    for lib in extraLibs(sparkCtx, sparkConf):
        print("        -", lib)

def help():
    print("""
Available magics (Spark 2 only !):
    %%sparkStatus
    %%sql --status  -v|--view  -p|--pandas <var>  -s|--spark <var>
    %%mysql -p|--pandas pdf
""")
