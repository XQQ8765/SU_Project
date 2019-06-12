/*
* Run the script in "Federation mater" FMS to change "enable_spi_collections" to "true"/"false" for "DBO_ASP_SPI_Config" topology objects.
* Note that: If you would like to enable the SPI for the oracle DB agnets, please list their agent-names into line "def enableSPIAgentNames =...", otherwise the "enable_spi_collections_flag" will be set to false.
* From SU-6656.
*/
def scriptName = "enable_spi_collections.groovy"
def output = new StringBuilder()

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

def findAll_DBO_ASP_SPI_Config = {
	return #$objectsbytype(DBO_ASP_SPI_Config)#.topologyObjects
}

def output_DBO_ASP_SPI_Config = { prefix, dboASPSpiConfigObj ->
	if (dboASPSpiConfigObj == null) {
		log ("${prefix} is null")
		return
	}
	log ("${prefix}.name:${dboASPSpiConfigObj.name}")
	log ("${prefix}.uniqueId:${dboASPSpiConfigObj.uniqueId}")
	log ("${prefix}.controllingDomainRoot.name:${dboASPSpiConfigObj.controllingDomainRoot?.name}")
}

def shouldEnableSPICollections = { dbAgentName ->
    //Put the agent names here which should enable SPI
    def enableSPIAgentNames = ['10.1.252.53_IFRSPROD', '10.1.252.146_BRMPROD']//For customer FMS
	//def enableSPIAgentNames = ['RAC-10.30.155.233-RAC', 'ZHUOL7FDB12C2012.melquest.dev.mel.au.qsft-RAC2', 'ZHUOL7FDB12C2011.melquest.dev.mel.au.qsft-RAC1', '10.10.187.18-o12102cdb', '10.10.187.74-ORCL11']//For dev fms http://10.30.171.171:8080
	def enableSPI = enableSPIAgentNames?.any {
		it?.equalsIgnoreCase(dbAgentName)
	}
	log("Agent :${dbAgentName} -> enableSPI:${enableSPI}")
	return enableSPI
}

def modify_enable_spi_collections = { dboASPSpiConfigObj ->
	output_DBO_ASP_SPI_Config("DBO_ASP_SPI_Config", dboASPSpiConfigObj)
	if (dboASPSpiConfigObj == null) {
		return
	}
	def srvData = server.DataService
	def srvCF = server.CanonicalFactory
	def samplePeriod = 60 * 60 * 1000// 1 hour
	def enable_spi_collections_flag = shouldEnableSPICollections(dboASPSpiConfigObj.controllingDomainRoot?.name)
	srvData.publish(srvCF.createNode(dboASPSpiConfigObj, "enable_spi_collections", ""+enable_spi_collections_flag, samplePeriod));
	log("Set \"DBO_ASP_SPI_Config.enable_spi_collections = ${enable_spi_collections_flag}\" successfully.")
	log('')
}

def execute = {
	def dboASPSpiConfigObjs = findAll_DBO_ASP_SPI_Config()

	if (isCollectionBlank(dboASPSpiConfigObjs)) {
		log("No DBO_ASP_SPI_Config instance exists in the FMS.")
		return
	} else {
		log("${dboASPSpiConfigObjs?.size()} DBO_ASP_SPI_Config instances exists in the FMS.")
	}
	log("\n")
	dboASPSpiConfigObjs?.each {
		modify_enable_spi_collections(it)
	}
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
