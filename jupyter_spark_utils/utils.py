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

import sys


def extraLibs(conf):
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
    extra_libs = []
    for lib in conf.get("spark.executorEnv.PYTHONPATH").split("<CPS>"):
        extra_lib = "/".join(lib.split("/")[1:])
        for ex in excludes:
            if extra_lib.startswith(ex):
                extra_lib = None
                break
        if extra_lib is not None:
            extra_libs.append(extra_lib)
    return (extra_libs)


def overview():
    sparkCtx = sys.modules["pyspark"].SparkContext.getOrCreate()
    sparkConf = sparkCtx.getConf()
    print("Python:", sys.version.replace("\n", "- "))
    print("")
    print("Spark:  %s (master: %s, executors: %s, executor memory: %s)" % (sparkCtx.version, 
                                                                           sparkConf.get("spark.master"),
                                                                           sparkConf.get("spark.executor.instances"), 
                                                                           sparkConf.get("spark.executor.memory")))
    print("") 
    print("Spark Extra Libs:")
    for lib in sorted(extraLibs(sparkConf)):
        print("        -", lib)

