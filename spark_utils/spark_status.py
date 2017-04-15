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


import requests
from IPython.display import Javascript, HTML, display_html, display_javascript


def prepareSparkStatus():
    style = "background-color: rgba(255,255,255,0.9); z-index:100;" +\
            "position: fixed;" +\
            "left: 0; right: 0; bottom: 0; height: 45px;" +\
            "padding: 10px"
    js = """
    if (typeof(window.myTimers) == "undefined") {
        window.myTimers = []
    }
    
    $("#sparkStatusFooter").remove()

    $('body').append('<div id="sparkStatusFooter" style="%s"></div>');
    """ % style
    display_javascript(Javascript(js))


def hideSparkStatus():
    display_javascript(Javascript("""$("#sparkStatusFooter").hide()"""))

    
def showSparkStatus():
    display_javascript(Javascript("""$("#sparkStatusFooter").show()"""))

    
def removeSparkStatus():
    display_javascript(Javascript("""$("#sparkStatusFooter").remove()"""))

    
def stopSparkStatus():
    js = """
    for (var i in window.myTimers) {
        clearInterval(window.myTimers[i])
    }
    window.myTimers = []
    $("#sparkStatusFooter").remove()
    """
    display_javascript(Javascript(js))

    
def startSparkStatus(uiWebUrl, applicationId):
    
    # Spark provide the RM UI Web Url, which redirects to the resource manager
    response = requests.get("%s" % uiWebUrl, allow_redirects=False)
    resourceManager = response.headers["Location"].split("/")[2]
    
    js = """
    var sparkStatus = function(rm, appId) {
        var url = "http://" + rm + "/proxy/" + appId;
        var killPath = "/jobs/job/kill/?id=";
        var trackPath = "/api/v1/applications/" + appId + "/";
        
        window.sparkKillJob = false;
        
        var killJob = function(jobId) {
            $.ajax({
                url: url + killPath + jobId,
                success: function(response) {
                    console.log("kill success");
                },
                error: function(request, options, error) {
                    console.log("kill error ", error)
                }
            });  
        }

        var loadStatus = function(statusType, callback) {
            $.ajax({
                dataType: "json",
                url: url + trackPath + statusType,
                username: "bernhard",
                success: callback,
                error: function(request, options, error) {
                    console.log("load error ", error)
                }
            });                
        }

        var showStatus = function(txt, withButton) {
            var button = "";
            if (withButton) {
                button = '<button style="margin:4px;" onclick="window.sparkKillJob=true"> KILL </button>'
            }
            $("#sparkStatusFooter").html(button + txt);
        }

        var retries = 3;
        var attempt = 0;
        
        window.myTimer = setInterval(function() {
            console.log("next")
            loadStatus("jobs", function(data) {
                var complete = true;
                for (var i in data) {
                    complete = complete && (data[i].status == "SUCCEEDED");
                }
                if (complete && attempt < 3) {
                    complete = false;
                    attempt++;
                } else {
                    attempt = 0;
                }
                if (!complete) {
                    var d = data[0]
                    var out = "SPARK STATUS: JobId: " + d.jobId +
                              ", Tasks (all/act/done): " + d.numTasks          + "/" + 
                                                           d.numActiveTasks    + "/" + 
                                                           d.numCompletedTasks +
                              ", stageIds: " + JSON.stringify(d.stageIds)

                    if (window.sparkKillJob) {
                        killJob(d.jobId);
                    }

                    loadStatus("stages", function(data) {
                        for (var i in data) {
                            if (data[i].status != "COMPLETE") {
                                var out2 = " ==> " +
                                           "Stage: " + data[i].stageId + 
                                           " (" + data[i].name.split(":")[0] + "): " + 
                                           data[i].status
                                showStatus(out + out2, true);
                            }
                        }
                    })
                } else {
                    showStatus("Done", false)
                    clearInterval(window.myTimer)
                }
            })
        }, 500)

        window.myTimers.push(myTimer)
    }
    
    sparkStatus("%s", "%s")
    """ % (resourceManager, applicationId)

    display_javascript(Javascript(js))
  