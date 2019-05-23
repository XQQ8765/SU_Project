/*
* Run the "Federation mater/children" FMS to print "DBSS_ASP_PI_Conn_Profile.enable_spi_collections" to "true"/"false" for "DBSS_ASP_PI_Conn_Profile" topology objects.
* From SU-6844.
*/
def scriptName = "enable_spi_collections.groovy"
def output = new StringBuilder()
ds = server.DataService

def log = { content ->
	println content
	output.append(content).append("\n")
}

def isCollectionBlank = { collection ->
    return (collection == null) || (collection.size() == 0)
}

def isStringBlank = { str ->
	return str == null || str.trim().length() == 0
}

def outputObservation = { prefix, observation ->
	if (observation == null) {
		log("${prefix} is null.")
		return
	}
	log("${prefix}(latest -> startTime) is: ${observation?.getStartTime()}") 
	log("${prefix}(latest -> endTime) is: ${observation?.getEndTime()}") 
	log("${prefix}(latest -> value) is: ${observation?.getValue()}")
}

def output_DBSS_ASP_PI_Conn_Profile = { prefix, dbssASPSpiConfigObj ->
	if (dbssASPSpiConfigObj == null) {
		log("${prefix} is null")
		return
	}
	log("${prefix}.name:${dbssASPSpiConfigObj.name}")
	log("${prefix}.uniqueId:${dbssASPSpiConfigObj.uniqueId}")
	//dbssASPSpiConfigObj.controllingDomainRoot is with type "DBSS_Agent_Model"
	log("${prefix}.controllingDomainRoot.name:${dbssASPSpiConfigObj.controllingDomainRoot?.name}")
	def enable_spi_collections_observation = ds.retrieveLatestValue(dbssASPSpiConfigObj, "enable_spi_collections")
	outputObservation("${prefix}.enable_spi_collections", enable_spi_collections_observation)
	log("")
}

def printAll_DBSS_ASP_PI_Conn_Profile = {
	def dbssASPSpiConfigObjs = #$objectsbytype(DBSS_ASP_PI_Conn_Profile)#.topologyObjects
	if (isCollectionBlank(dbssASPSpiConfigObjs)) {
		log("No \"DBSS_ASP_PI_Conn_Profile\" instances exists in the FMS.")
		return
	} else {
		log("${dbssASPSpiConfigObjs?.size()} DBSS_ASP_PI_Conn_Profile instances exists in the FMS.")
	}
	dbssASPSpiConfigObjs?.eachWithIndex { dbssASPSpiConfigObj, index ->
		output_DBSS_ASP_PI_Conn_Profile("DBSS_ASP_PI_Conn_Profile[${index}]", dbssASPSpiConfigObj)
	}
}

def execute = {
	printAll_DBSS_ASP_PI_Conn_Profile()
}

//The main logic
def t0 = System.currentTimeMillis()
log("Script ${scriptName} Start time is:${new Date()}")
log("\n")

execute()

def t1 = System.currentTimeMillis()
def spentTime = t1 - t0
log("The script ${scriptName} completes, spent ${spentTime} ms.")
return output.toString()
