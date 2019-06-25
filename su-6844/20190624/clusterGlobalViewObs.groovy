ds = server.DataService;
output = new StringBuilder()

def last10GVValues = ds.retrieveLastNValues(scope, "clustersGlobalViewObs", 10)
last10GVValues?.eachWithIndex { gvObservedValue, index ->
    output_GV_Observed_Value("clustersGlobalViewObs[${index}]", gvObservedValue)
}
return output.toString()

def log(content) {
    println content
    output.append(content).append("\n")
}
def output_GV_Observed_Value(prefix, gvObservedValue) {//"gvObservedValue" Type is "GV_Observed_Value"
    if (gvObservedValue == null) {
        log("${prefix} is null.")
        return
    }
    output_ObservedValue(prefix, gvObservedValue)
    def gvClusterDatas = gvObservedValue.getValue()
    log("${prefix}.value.value -> gvClusterDatas?.size():${gvClusterDatas?.size()}")
    log("")
}
def output_ObservedValue(prefix, observedValue) {
    if (observedValue == null) {
        log("${prefix} is null.")
        return
    }
    log("${prefix}.startTime is: ${observedValue.getStartTime()}")
    log("${prefix}.endTime is: ${observedValue.getEndTime()}")
    //log("${prefix}.value is: ${observedValue.getValue()}")
}