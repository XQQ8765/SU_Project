/**
 * The script is used to println last 10 GV_Observed_Value for "FMS_GV_Data.clustersGlobalViewObs".
 * The "scope" object should be "FMS_GV_Data".
 * The script is used in SU-6844.
 */
ds = server.DataService;
output = new StringBuilder()

outputTopologyBasic("FMS_GV_Data", scope)
log("")

def last10GVValues = ds.retrieveLastNValues(scope, "clustersGlobalViewObs", 10)
last10GVValues?.eachWithIndex { gvObservedValue, index ->
    output_GV_Observed_Value("FMS_GV_Data.clustersGlobalViewObs[${index}]", gvObservedValue)
}
return output.toString()

def log(content) {
    println content
    output.append(content).append("\n")
}

def outputTopologyBasic(prefix, topology) {
    if (topology == null) {
        log("${prefix} is null")
        return
    }
    log("${prefix}.name is: ${topology?.name}")
    log("${prefix}.effectiveStartDate: ${topology?.effectiveStartDate}")
    log("${prefix}.lastUpdated: ${topology?.lastUpdated}")
    log("${prefix}.uniqueId: ${topology?.uniqueId}")
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
    log("${prefix}.sampledPeriod is: ${observedValue.getSampledPeriod()}")
    log("${prefix}.periodStart to periodEnd: ${observedValue.getPeriodStart()} - ${observedValue.getPeriodEnd()}")
}