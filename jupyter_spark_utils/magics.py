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

from IPython.core.magic import Magics, magics_class, cell_magic, needs_local_scope
from .spark_status import prepareSparkStatus, startSparkStatus, stopSparkStatus
import getopt


@magics_class
class SparkMagics(Magics):

    @needs_local_scope
    @cell_magic
    def sparkStatus(self, line, cell, local_ns=None):
        glob = self.shell.user_ns
        sparkContext = glob["sc"]
        prepareSparkStatus()
        startSparkStatus(sparkContext.uiWebUrl, sparkContext.applicationId)
        self.shell.run_cell(cell, store_history=False)
        stopSparkStatus()


    @needs_local_scope
    @cell_magic
    def sql(self, line, cell, local_ns=None):
        glob = self.shell.user_ns
        sparkContext = glob["sc"]
        spark = glob.get("spark", glob["sqlContext"])

        try:
            opts, args = getopt.getopt(line.split(" "), "s:p:v", ["spark=", "pandas=", "status", "view"])
        except getopt.GetoptError as err:
            opts = []

        status = False
        view = False
        pandasDf = None
        sparkDf = None

        for o, a in opts:
            if o == "--status":
                status = True
            elif o in ("-v", "--view"):
                view = True
            elif o in ("-p", "--pandas"):
                pandasDf = a
            elif o in ("-s", "--spark"):
                sparkDf = a
            else:
                assert False, "unhandled option"

        if status:
            prepareSparkStatus()
            startSparkStatus(sparkContext.uiWebUrl, sparkContext.applicationId)

        result = spark.sql(cell)

        if pandasDf is not None:
            glob[pandasDf] = result.toPandas()

        if sparkDf is not None:
            glob[sparkDf] = result

        if status:
            stopSparkStatus()

        if view:
            return result.toPandas()
        else:
            return result


    @needs_local_scope
    @cell_magic
    def mysql(self, line, cell, local_ns=None):
        import MySQLdb
        import pandas as pd

        glob = self.shell.user_ns
        try:
            opts, args = getopt.getopt(line.split(" "), "p:", ["pandas="])
        except getopt.GetoptError as err:
            opts = []

        pandasDf = None

        for o, a in opts:
            if o in ("-p", "--pandas"):
                pandasDf = a
            else:
                assert False, "unhandled option"

        if "mysqlConfig" in glob:
            db = MySQLdb.connect(**glob["mysqlConfig"])
            c = MySQLdb.cursors.DictCursor(db)
            c.execute(cell)
            result = list(c.fetchall())

            if pandasDf is not None:
                glob = self.shell.user_ns
                glob[pandasDf] = pd.DataFrame(result)
            else:
                return pd.DataFrame(result)
        else:
            print('Missing config: mysqlConfig = dict(host="...",user="...",passwd="...",db="...")')

