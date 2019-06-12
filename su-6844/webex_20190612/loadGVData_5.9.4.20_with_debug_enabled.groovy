/**
 * This software is confidential. Quest Software Inc., or one of its subsidiaries, has supplied this software to you
 * under terms of a license agreement, nondisclosure agreement or both.
 *
 * You may not copy, disclose, or use this software except in accordance with those terms.
 *
 * Copyright 2017 Quest Software Inc. ALL RIGHTS RESERVED.
 *
 * QUEST SOFTWARE INC. MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE,
 * OR NON-INFRINGEMENT. QUEST SOFTWARE SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */
package system._dbwc.scripts;

/*
* Load the GV Data.
* @return Monitoring:DBWC_GV_GlobalViewRoot
*/

import com.quest.qsi.javaGlobalView.logger.*;
import java.sql.Timestamp;
import com.quest.nitro.service.sl.ServiceLocatorFactory;
import com.quest.qsi.javaGlobalView.foglightUtils.QueryServiceUtils;
import com.quest.qsi.javaGlobalView.foglightUtils.DataServiceUtils;
import com.quest.wcf.data.wcfdo.SpecificTimeRange;
import org.apache.commons.lang.time.StopWatch;

//CAT = com.quest.qsi.fason.framework.fmslogger.LogFactory.getLogForWCF(functionHelper.getFunctionId())
class EnableDebugLog4JLogger extends org.apache.commons.logging.impl.Log4JLogger {
    EnableDebugLog4JLogger(name) {
        super(name)
    }

    public boolean isDebugEnabled() {
        return true
    }
    public void debug(String message) {//This method will enter if parameter type is "String" rather than "Object" in WCF groovy function
        info("DEBUG: ${message}")
    }

    public boolean isTraceEnabled() {
        return false
    }
    public void trace(String message) {
        info("TRACE: ${message}")
    }
}
CAT = new EnableDebugLog4JLogger("script.loadGVData.su_6844_v1")

DEBUG_ENABLED = CAT.isDebugEnabled()
TRACE_ENABLED = CAT.isTraceEnabled()

stopWatch = new StopWatch()//Calculate the time spent of this script
stopWatch.start()
if (DEBUG_ENABLED) {
    CAT.debug("loadGVData() - enter.")
}
// call the batch query function
functionHelper.invokeFunction("system:dbwc.dbBatchQuery");

//services
topologyService  = server.get("TopologyService");
queryService     = server.get("QueryService");
agentService     = server.get("AgentService");
dataService      = server.get("DataService");
registryService  = server.get("RegistryService")
dataServiceUtils = DataServiceUtils.getDataServiceUtils();

//specificTimeRange = functionHelper.getSpecificTimeRange();
specificTimeRange = createLastHourSpecificTimeRange();

gvData = loadGVLastDataObject();//"gvData" is with type "DBWC_GV_GlobalViewRoot"

def isLog         = registry("DBWC_Log_Common")

/****************************** Functions ******************************/

RUNNING_STATE_COLLECTING_DATA  = "5.0";
BLACKOUT_STATE_COLLECTING_DATA = "17.0";

/*
  This class is responsible to retrieve the upgrade status for the global view
*/
class UpgradeCheck {
    static cartridgeVersionCache = [:]
    static agentService          = ServiceLocatorFactory.getLocator().getAgentService()
    static cartridgeService      = ServiceLocatorFactory.getLocator().getCartridgeService()

    def isLog           = false

    def UpgradeCheck(_is_log){
        isLog = _is_log
    }

    static clearCartridgeVersionCache(){
        cartridgeVersionCache = [:]
    }

    def DBWC_Log(prefix, msg){
        if( isLog ){
            println ("DBWC_Log: " + msg);
        }
    }

    /*
            *  check if agent requires upgrade
            */
    def isUpgradeRequired(agent, agentMap){
        def start  = System.currentTimeMillis();
        def result = false;

        def theAgent = agentMap[agent.get("agentID").toString()];

        def cartridgeName = agent.get("type");

        if (cartridgeName == "DB_Oracle_RAC" || cartridgeName == "DB_Oracle_RAC_Instance"){ //care about Oracle
            cartridgeName = "DB_Oracle";
        }
        else if (cartridgeName == "DB_Azure_Instance" || cartridgeName == "DB_Azure_Database"){ //care about Azure
            cartridgeName = "DB_Azure";
        }
        else { //other carts
            cartridgeName = agent.get("type");
        }

        def agentVersion     = agent.get("agentVersion");
        if( theAgent != null ){
            agentVersion = theAgent.getVersion();
        }

        def cartridgeVersion = getCartridgeVersionByName(cartridgeName);
        result               = isNewerVersion(cartridgeVersion, agentVersion);
        if(cartridgeName.equals("MySQLAgent") || cartridgeName.equals("PostgreSQLAgent") || cartridgeName.equals("MongoDBAgent") || cartridgeName.equals("CassandraAgent"))
            result = false;

        def logMsg = "Global View - get all node entities: isUpgradeRequired(): cartridgeName = " + cartridgeName + ", agentVersion = " + agentVersion + " , cartridgeVersion = "+ cartridgeVersion + ", result = " + result + " , agent = " + agent;
        if(DBWC_LogFactory.getCurrentLogLevelByModule(DBWC_Module.DBWC_GLOBALVIEW) == DBWCLog_Level.DBWC_DEBUG){
            DBWC_LogFactory.getLog(DBWC_Module.DBWC_GLOBALVIEW).writeMessage(DBWCLog_Level.DBWC_DEBUG, logMsg);
        }
        def end = System.currentTimeMillis()
        def total = end - start
        //println "Global View - get all node entities: total time is Upgrade Required?: $total"

        return result;
    }

    static isNewerVersion(cartVer, customVer){
        if ((cartVer == null) || (customVer == null)) {
            return false;
        }

        def cartVerArr = cartVer.tokenize(".");
        def customVerArr = customVer.tokenize(".");

        /**adding the missing cells */
        if (cartVerArr.size()<customVerArr.size()){
            def diff = customVerArr.size()-cartVerArr.size();
            for (int i=0; i<diff; i++){
                cartVerArr.add("0");
            }
        }
        else if (customVerArr.size()<cartVerArr.size()){
            def diff = cartVerArr.size()-customVerArr.size();
            for (int i=0; i<diff; i++){
                customVerArr.add("0");
            }
        }

        /* checking the support status */
        def isSupported = false;
        def num         = 0;
        for (int i=0; (i<cartVerArr.size()) || (i<customVerArr.size()); i++) {
            if (i < cartVerArr.size()) {
                num = Integer.parseInt(cartVerArr[i]);
            }
            else {
                num = 0;
            }
            if (num < customVerArr[i]) {
                isSupported = false;
                break;
            }
            if (num > customVerArr[i]) {
                isSupported = true;
                break;
            }
        }
        return isSupported;
    }

    static getCartridgeVersionByName(cartridgeName) {

        def cartVersion = cartridgeVersionCache[cartridgeName]

        if( cartVersion == null ){
            def cart = cartridgeService.listCartridges().find{
                it.getIdentity().getName() == cartridgeName && it.getCartridgeStatus() == it.getCartridgeStatus().ACTIVATED
            }
            if (cart!=null){
                cartVersion = cart.getIdentity().getVersion();
                cartridgeVersionCache.put(cartridgeName, cartVersion);
            }
        }

        return cartVersion
    }
}//end of UpgradeCheck class

class OracleAgentContainer {
    def oracleAgent;
    def racInstanceAgents = new HashSet();
}

def calcStealthCollectStatus(isPaConfiguredList, aPaUsabilityList) {
    _error = false;
    _down = false;

    for (aPaUsability in aPaUsabilityList)
    {
        if (aPaUsability != null)
        {
            if (((collector_state = aPaUsability.get("collector_state",specificTimeRange ).get("latest",specificTimeRange )) != null) && (collector_state.get("value",specificTimeRange ).get("value",specificTimeRange ) == 1))
            {
                _down = true
                break
            }
            else {
                _down = false
            }
        }
    }
    boolean _isPaConfigured = 1
    for (isPaConfigured in isPaConfiguredList)
    {
        if (!isPaConfigured)
        {
            _isPaConfigured = 0
            break
        }
    }

    if (_isPaConfigured)
    {
        if (_down) result = "3"
        else  if (_error) result  = "2"
        else  result = "1"
    }
    else
    {
        result = "0"
    }
    return result
}


// The purpose of the function is to return status of StealthCollect
// Possible statuses: not configured, sc_ok, sc_error and sc_down
// The methods supports RAC
//
// Parameters:
//   aPaUsabilityList - List of PA Usability topologies
//   isPaConfiguredList - List of booleans (configured/not configured) for asp_config/pa_conn_profile/enable_pa_collections
// Author: Dan Ariely (This function is also availablt in the instances home pages)
def getSCStatus(aPaUsabilityList, isPaConfiguredList, aDbPlatformName, isNewVersion, minimalSCVersion){
    return functionHelper.invokeFunction("system:dbwc.getSCStatus", aPaUsabilityList, isPaConfiguredList, aDbPlatformName, isNewVersion, minimalSCVersion)
}


upgradeCheck = new UpgradeCheck(isLog)

def createTopologyObject = { aType ->
    return topologyService.createObject(topologyService.getType(aType));
}

def createDataObject( aType ){
    return topologyService.createAnonymousDataObject(topologyService.getType(aType));
}

def createClusterIdentifier( agentName, dbType, _isCluster ){
    def _clusterIdentifier = createDataObject("DBWC_GV_ClusterIdentifier");

    _clusterIdentifier.set("databaseType", dbType);
    _clusterIdentifier.set("IsClustered", _isCluster);
    _clusterIdentifier.set("agentName", agentName);

    return _clusterIdentifier;
}

def updateClusterIdentifier( agentName, _clusterIdentifier ){
    _clusterIdentifier.set("agentName", agentName);
    return _clusterIdentifier;
}

def createInstanceIdentifier( hostName, instanceName, port, aClusterIdentifier){
    def _instanceIdentifier = createDataObject("DBWC_GV_InstanceIdentifier");

    _instanceIdentifier.set("cluster", aClusterIdentifier);
    _instanceIdentifier.set("alias", aClusterIdentifier.getString("agentName"));
    _instanceIdentifier.set("hostName", hostName);
    _instanceIdentifier.set("InstanceName", instanceName);
    _instanceIdentifier.set("port", port);
    _instanceIdentifier.set("isAggregated", false);

    return _instanceIdentifier;
}

def updateInstanceIdentifier( _instanceIdentifier, hostName, instanceName, port, aClusterIdentifier ){
    _instanceIdentifier.set("cluster", aClusterIdentifier);
    _instanceIdentifier.set("alias", aClusterIdentifier.getString("agentName"));
    _instanceIdentifier.set("hostName", hostName);
    _instanceIdentifier.set("InstanceName", instanceName);
    _instanceIdentifier.set("port", port);
    _instanceIdentifier.set("isAggregated", false);

    return _instanceIdentifier;
}

def createSystemUtilization ( aAgent, agentModel , aInstanceData , key ){
    gvSeverity = functionHelper.invokeFunction("system:dbwc.37", "DBWC_GV_GlobaViewSeverityStatus", aAgent, "current", key);
    aInstanceData.set("gvSeverity", gvSeverity);
    if (gvSeverity != null ) {
        if (agentModel.get("runningState").value.equals(RUNNING_STATE_COLLECTING_DATA)) {
            return gvSeverity.get("totalStatus").get("componentSeverity");
        } else {
            gvSeverity.get("totalStatus").set("componentSeverity", "-1");
            return "-1";
        }
    }
}

/**
 * stuff each sub-version separated by dot with leading zeroes
 * 11.2.0.1.0 -> 00011.00002.00000.00001.00000.
 * 9.00.4060.00 -> 00009.00000.04060.00000.00000.
 * @param version the version to be stuffed
 * @return the updated stuffed version
 */
def getZeroStuffedVersion(version) {
    version = version?.trim();
    // if the version is empty or contains spaces (' ') between its characters (Sybase version is as follows: 'ASE 15.0 2 EBF 14332') - no stuffing will be done
    if ((version == null) || (version.isEmpty()) || version.contains(" ")) {
        return version;
    }

    def MAX_SIZE = 5;
    def subCount = 0;
    def stuffedVersion = new StringBuilder();
    def versions = version.split("\\.");
    for (subVersion in versions) {
        def verLength = subVersion.length();
        while (verLength++ < MAX_SIZE) stuffedVersion.append("0");
        stuffedVersion.append(subVersion);
        stuffedVersion.append(".");
        subCount++;
    }

    while (subCount++ < MAX_SIZE) {
        def verLength = 0;
        while (verLength++ < MAX_SIZE) stuffedVersion.append("0");
        stuffedVersion.append(".");
    }

    return stuffedVersion.toString();
}

/**
 * Update in the cluster's instance agent state according to the cluster one
 * in case it is not in collecting data state
 * @param clusterData the cluster data topology object
 */
def updateClusterInstancesRunningState(clusterData) {
    def runningState = clusterData.get("agentState");
    if (!runningState.equals(RUNNING_STATE_COLLECTING_DATA)) {
        def instanceDataList = clusterData.get("instances");
        if (instanceDataList != null) {
            for (instanceData in instanceDataList) {
                instanceData.set("agentState", runningState);
            }
        }
    }
}

def getAgentRunningState(agentTopology, agentInstance) {
    def runningState = "0.0" // Unknown as default prior to check;

    if(!isFederation){
        if (agentInstance != null) {
            if (agentInstance.getIsBlackedOut()) {
                runningState = BLACKOUT_STATE_COLLECTING_DATA;
            } else {
                runningState = Double.toString(agentInstance.getState().getStateCode());
            }
        } else {
            runningState = agentTopology?.get("runningState")?.value;
        }

        if (runningState == null) {
            runningState = "0.0";
        }
    } else {
        //avoid checking agent state on federator FMS since its known to cause uunnecessaryround trip to FMS in turn and cause prolong script execution time
        runningState = "5.0";
    }

    return runningState;
}

def fillIsUpgradeRequired(instanceData, agent) {
    def isUpgradeRequired = false
    if (agent != null) {
        try {
            // compare agent and cartridge versions
            isUpgradeRequired = upgradeCheck.isUpgradeRequired(agent, realAgentMap);

            def agentType = agent.get("topologyTypeName");

            //if the agent and cartridge version are the same check that the upgrade wizard completed per agent.
            //check the agent upgrade flag only if the fglam level reported that at least 1 agent was not upgrade
            //to prevent extra calls to the fms repository when all agents are upgraded (normal state)
            if (!isUpgradeRequired && agentType.equals("DBSS_Agent_Model")) {
                //make sure that Is_Fglams_Required_Upgrade registry flag
                if (gIsDBSSFglamsRequiredUpgrade > 0) {
                    def statusUpgrade = dataService.retrieveLatestValue(agent, "isUpgradeRequired")?.value;
                    isUpgradeRequired = (statusUpgrade != null) && Boolean.valueOf(statusUpgrade);
                    //store the fglam upgrade state based on all agents isUpgradeRequired flag
                    gDBSSAgentsUpgradeNotCompleted = gDBSSAgentsUpgradeNotCompleted || isUpgradeRequired;
                }
            } else if(agentType.equals("DBSS_Agent_Model")){
                //agent is by default not upgraded if its version is diffent the cartirdge version
                gDBSSAgentsUpgradeNotCompleted = true;
            }

            if (!isUpgradeRequired && agentType.equals("DBO_Agent_Model")) {
                if (gIsDBOFglamsRequiredUpgrade > 0) {
                    def statusUpgrade = dataService.retrieveLatestValue(agent, "isUpgradeRequired")?.value;
                    isUpgradeRequired = (statusUpgrade != null) && Boolean.valueOf(statusUpgrade);
                    //store the fglam upgrade state based on all agents isUpgradeRequired flag
                    gDBOAgentsUpgradeNotCompleted = gDBOAgentsUpgradeNotCompleted || isUpgradeRequired;
                }
            } else if(agentType.equals("DBO_Agent_Model")){
                //agent is by default not upgraded if its version is diffent the cartirdge version
                gDBOAgentsUpgradeNotCompleted = true;
            }

            if (!isUpgradeRequired && agentType.equals("DB2_Agent_Model")) {
                if (gIsDB2FglamsRequiredUpgrade > 0) {
                    def statusUpgrade = dataService.retrieveLatestValue(agent, "isUpgradeRequired")?.value;
                    isUpgradeRequired = (statusUpgrade != null) && Boolean.valueOf(statusUpgrade);
                    //store the fglam upgrade state based on all agents isUpgradeRequired flag
                    gDB2AgentsUpgradeNotCompleted = gDB2AgentsUpgradeNotCompleted || isUpgradeRequired;
                }
            } else if(agentType.equals("DB2_Agent_Model")){
                //agent is by default not upgraded if its version is diffent the cartirdge version
                gDB2AgentsUpgradeNotCompleted = true;
            }
        } catch (Exception e) {
            printDebugModeLog("Exception while checking if upgrade is required for agent with name - " + agent.getName() + " " + e.getMessage(), null);
        }
    }

    instanceData.set("isUpgradeRequired", isUpgradeRequired);
}


/*
	This function is responsible of taking all in the inserted parameters
	and put them in the 'aInstanceData' object.
*/
def fillInstanceData(aInstance, aAgent, dbVersion, upsince, hostName, instanceName, port,
                     aClusterIdentifier, aInstanceData, aAggregated,
                     aSystemSeverity, topologiesForAlarms, mCpu, mMemory, mDisk, mPAStatus,
                     isEnableVMCollections, osMonitoringState,isReportingServices,
                     hostTopologysForAlarms, host) {

    // Identifier parent_node
    if (aInstanceData.get("instanceIdentifier") == null) {
        aInstanceData.set("instanceIdentifier", createInstanceIdentifier(hostName, instanceName, port, aClusterIdentifier));
    } else {
        updateInstanceIdentifier(aInstanceData.get("instanceIdentifier"), hostName, instanceName, port, aClusterIdentifier);
    }

    if ((dbVersion != null) && (dbVersion.equalsIgnoreCase("unknown"))) {
        dbVersion = "";
    }

    topologiesForAlarms_uid = [];
    for(obj in topologiesForAlarms) {
        try{
            refObj = topologyService.getObject(obj.getUniqueId());
            topologiesForAlarms_uid.add(refObj);
        } catch(e) {}
    }

    hostTopologysForAlarms_uid = [];
    for(obj in hostTopologysForAlarms) {
        try{
            refObj = topologyService.getObject(obj.getUniqueId());
            hostTopologysForAlarms_uid.add(refObj);
        } catch(e) {}
    }

    //Global
    aInstanceData.set("isPAAgentUp", false);
    aInstanceData.set("databaseVersion", dbVersion);
    aInstanceData.set("stuffedDBVersion", getZeroStuffedVersion(dbVersion));
    aInstanceData.set("databaseUpSince", upsince);
    aInstanceData.set("topologyObject", aInstance);
    aInstanceData.set("topologysForAlarms", topologiesForAlarms_uid);
    aInstanceData.set("hostTopologysForAlarms", hostTopologysForAlarms_uid);
    aInstanceData.set("agent", aAgent);
    aInstanceData.set("CPUUtilization", mCpu );
    aInstanceData.set("MemoryUtilization", mMemory);
    aInstanceData.set("DiskUtilization" ,  mDisk);
    aInstanceData.set("instanceWorkingMode", "Full");
    aInstanceData.set("isEnableVMCollections" , isEnableVMCollections);
    aInstanceData.set("osMonitoringState" , osMonitoringState);
    aInstanceData.set("isDataExist" , true); //default value
    aInstanceData.set("paState", mPAStatus);
    aInstanceData.set("isReportingServices", isReportingServices);
    if (host != null) {
        host = functionHelper.invokeFunction("system:dbwc_globalview.RetrieveFreshHost", host);
        aInstanceData.set("host", host);
    }


    def agentInstance = null;
    if (aAgent != null) {
        agentInstance = gAllAgentMap.get(String.valueOf(aAgent.get("agentID")));
    }

    def runningState = getAgentRunningState(aAgent, agentInstance);
    aInstanceData.set("agentState", runningState);

    fillIsUpgradeRequired(aInstanceData, aAgent);
    fillVMWareInfo(aInstanceData, aAgent);

    // agent running and collecting data (status == OK )
    if (runningState.equals(RUNNING_STATE_COLLECTING_DATA) || runningState.equals(BLACKOUT_STATE_COLLECTING_DATA)) {
        def aAgentTplName = aAgent.get("topologyTypeName");
        if (!aAgentTplName.contains("Sybase_MDAAgent") &&
                !aAgentTplName.contains("Sybase_RSAgent") &&
                !aAgentTplName.contains("DB2Agent") &&
                !aAgentTplName.contains("PostgreSQLAgent")) {

            def isDealingWithRAC = aInstance?.getType()?.getName()?.contains("Clusters_Data") ||
                    aInstance?.getType()?.getName()?.contains("MongoServer") ||
                    aInstance?.getType()?.getName()?.contains("MongoReplicaSet") ||
                    aInstance?.getType()?.getName()?.contains("Mongo_Cluster");
            def isStandaloneInstance = true;

            if (isDealingWithRAC) {
                isStandaloneInstance = false;
            }
            else {
                /* DBSS_Clusters_Data doesn't have "is_rac" property */
                try{
                    isStandaloneInstance = !aInstance?.parent_node?.is_rac;
                }catch(e){}
            }
            if (isStandaloneInstance) {
                /* take from cluster, which also includes 'database' */
                def aInstanceTplName = aInstance?.get("topologyTypeName");
                if (!aInstanceTplName.equals("DBSS_Azure_Instance") &&
                        !aInstanceTplName.equals("DBSS_Azure_Database") &&
                        !aInstanceTplName.equals("DB2_Instance") && !aInstanceTplName.equals("DB2_Database") && !aInstanceTplName.equals("DB2_Agent_Model") &&
                        !aInstanceTplName.equals("DBSS_Agent_Model") &&
                        !aInstanceTplName.equals("DBO_Agent_Model") &&
                        !aInstanceTplName.equals("SS_Integration_Service")&&
                        !aInstanceTplName.equals("CassandraCluster")&&
                        !aInstanceTplName.equals("CassandraNode")&&
                        !aInstanceTplName.equals("SS_Reporting_Services")) {
                    aInstanceData.set("topologyObject", aInstance?.parent_node);
                }
            }
        }

        def totalHealth = getSeverityForGlobalViewNode(topologiesForAlarms);
        aInstanceData.set("totalHealth", gSeverityTypeMap.get(totalHealth));
        aInstanceData.set("systemHealth", gSeverityTypeMap.get(aSystemSeverity));
    }
    else {
        // agent not running   (status ==  problem )
        if (runningState == "0.0" ){
            //putting a severity of n/a
            aInstanceData.set("totalHealth", gSeverityTypeMap.get(-1));
        }
        else{
            aInstanceData.set("totalHealth", gSeverityTypeMap.get(4));
        }
        aInstanceData.set("systemHealth", gSeverityTypeMap.get(-1));
    }
}

/*
This function returns an Integer value with the max severity of the alarms in the gevin topologiesForAlarms list
Params:
topologiesForAlarms - a list of topologies that we need to know thier max alarm severity
**/
def getSeverityForGlobalViewNode(topologiesForAlarms){
    def FATAL = 4;
    def CRITICAL = 3;
    def WARNING = 2;

    def maxSeverity = 0; //green
    def tplSeverity = 0;

    for (tpl in topologiesForAlarms) {
        if (tpl!=null){
            if (tpl.get("alarmAggregateFatalCount", specificTimeRange)>0){
                tplSeverity = FATAL;
            }
            else if (tpl.get("alarmAggregateCriticalCount", specificTimeRange)>0){
                tplSeverity = CRITICAL;
            }
            else if (tpl.get("alarmAggregateWarningCount", specificTimeRange)>0){
                tplSeverity = WARNING;
            }
            maxSeverity = Math.max(maxSeverity, tplSeverity);
        }
    }
    return maxSeverity;
}


/*
	This function returns the value of the PA status from the 'clusterData' object of a RAC instance
*/
def getInstancePAStatusForRAC (clusterData){
    try {
        result = [];
        if (clusterData != null) {
            agentModels = clusterData.get("instances").agent;
            if (agentModels != null) {
                for (agentModel in agentModels) {
                    if (agentModel != null) {
                        asp_config = agentModel.get("asp_config");
                        if (asp_config != null) {
                            pa_conn_profile = asp_config.get("pa_conn_profile");
                            if (pa_conn_profile != null) {
                                def enable_pa_collectionsLatest = dataServiceUtils.getStringObservationLatestData(pa_conn_profile, "enable_pa_collections");
                                result.add(Boolean.toString(enable_pa_collectionsLatest.toBoolean()));
                            }
                        }
                    }
                }
            }
        }
        return result;
    }
    catch (e){
        printDebugModeLog ("Failed to get PA status for RAC Node: " + clusterData.getName(), null);
    }
}



/*
	This function returns the value of the PA status from the 'clusterData' object.
*/
def getInstancePAStatus (clusterData){
    result = [];
    if (clusterData != null) {
        agentModels = clusterData.get("agent");
        if (agentModels != null) {
            for (agentModel in agentModels) {
                if (agentModel != null) {
                    asp_config = agentModel.get("asp_config");
                    if (asp_config != null) {
                        pa_conn_profile = asp_config.get("pa_conn_profile");
                        if (pa_conn_profile != null) {
                            def enable_pa_collectionsLatest = dataServiceUtils.getStringObservationLatestData(pa_conn_profile, "enable_pa_collections");
                            result.add(enable_pa_collectionsLatest.toBoolean());
                        }
                    }
                }
            }
        }
    }
    return result;
}

/**
 * Replace the host name to pseudo host name when the DB2 instance is reside on PurScale or Partitioned environment
 * @param dbInstance - Reference to DB2 Instance topology
 * @param hostName   - Specify the given default host name that retrieve from the DB2 instance level
 * @return
 *  In case of PurScale Instance - return "pureScale"
 *  In case of Partitioned environment return "Partitioned"
 *  otherwise return the given host name (of the instance level)
 */
def replaceDB2HostnameIfNeeded(dbInstance, hostName) {
    def result = hostName;
    try {
        def instanceGeneral = dbInstance.get("instance_general");
        def instanceActivity = dbInstance.get("instance_activity");
        if (instanceGeneral == null) {
            printDebugModeLog ("Not expected to get null value for instance_general property associated with instance (" + dbInstance.get("name") + ")", null);
        } else {
            //Check if the instance is pure scale
            def is_pureScale = false;
            try {
                is_pureScale = instanceGeneral.get("is_pureScale");
            } catch (Exception e) {
                printDebugModeLog ("Failed to retrieve property (is_pureScale)" , e);
            }
            if (is_pureScale) {
                result = "pureScale";
            } else {
                def partitionsNumber = Integer.parseInt(dataServiceUtils.getStringObservationLatestData(instanceActivity, "num_nodes_in_db2_instance"));
                if (partitionsNumber>1) {
                    result = "Partitioned";
                }
            }
        }
    } catch (e) {
        printDebugModeLog ("Not expected to get null value for instance_general property associated with instance (" + dbInstance.get("name") + ")", null);
    }
    return result;
}

def getDB2OSMonitoringState(monitoringAgent, monitoredHost, osMetric) {
    def isMonitorOS = null;

    // check the topology asp property if exist
    def agentVersion = monitoringAgent?.get("agentVersion");
    if (UpgradeCheck.isNewerVersion(agentVersion, "5.6.10.1")) {
        def aspConfig = monitoringAgent?.get("asp_config");
        if (aspConfig) {
            try {
                isMonitorOS = dataServiceUtils.getStringObservationLatestData(aspConfig, "is_enable_os_collections").equals("true");
            } catch (Exception e) {
                // do nothing. the observation probably not exists
            }
        }
    }

    // check if OS metric was collected
    if (isMonitorOS == null) {
        isMonitorOS = (osMetric != null);
    }

    def isMonitorIC = isHostMonitoredByIC(monitoredHost);
    return getOSMonitoringState(isMonitorOS, isMonitorIC);
}


def buildParamsStrForCreateDB2InstanceData (dbInstance, gvClusterData, monitoringAgent) {
    def paramsStr = new StringBuilder()
    paramsStr << "paramsStr: [\n dbInstance:${DB2_Instance_toString(dbInstance)}\n, "
    paramsStr << "gvClusterData:${DBWC_GV_GVClusterData_ToString(gvClusterData)}\n, "
    paramsStr << "monitoringAgent:${Agent_ToString(monitoringAgent)}]"
    return paramsStr.toString()
}
/*
	This function creates a topology for single DB2 Ararat instance.
	# dbInstance 		DB2_Instance
	# gvClusterData 	DBWC_GV_GVClusterData
	# pAgent			DB2_Agent_Model
*/
def createDB2InstanceData (dbInstance, gvClusterData, monitoringAgent) {
    if (TRACE_ENABLED) {
        def paramsStr = buildParamsStrForCreateDB2InstanceData(dbInstance, gvClusterData, monitoringAgent)
        CAT.trace("DB2 - createDB2InstanceData(): enter. ${paramsStr}")
    }

    def clusterData = gvClusterData;
    if (clusterData == null){
        clusterData = createDataObject("DBWC_GV_GVClusterData");
    }

    // Identifier
    def db2AgentName = monitoringAgent.get("name");

    //create or update the identifier
    def clusterIdentifier;
    if (clusterData.instanceIdentifier != null) {
        clusterIdentifier = updateClusterIdentifier (db2AgentName, clusterData.instanceIdentifier.cluster);
    } else {
        def db2DBtype = gDBTypeMap.get("DB_DB2");
        clusterIdentifier = createClusterIdentifier (db2AgentName, db2DBtype, false);
    }

    clusterData.set("clusterIdentifier", clusterIdentifier);
    clusterData.setName(monitoringAgent.get("name"));

    def hostTopologysForAlarms = [];
    try{
        hostTopologysForAlarms.add(monitoringAgent.get("monitoredHost"));
    }catch(e){}

    def dev_instance_activity = dbInstance?.get("instance_activity")
    if (TRACE_ENABLED) {
        CAT.trace("DB2 - createDB2InstanceData(): dev_instance_activity:${dev_instance_activity}.")
    }
    if ((dbInstance != null) && (dev_instance_activity != null)) {
        def instanceActivity = dev_instance_activity
        def hostName = dbInstance.get("monitoredHost").get("name");

        def instanceName = dbInstance.get("instance_name");
        def dbVersion = ""
        try {
            dbVersion = dataServiceUtils.getStringObservationLatestData(instanceActivity, "product_name");
        } catch (e) {
            CAT.error("DB2 - createDB2InstanceData(): Exception happens when call \"instanceActivity.product_name\". dbInstance:${DB2_Instance_toString(dbInstance)}.", e)
        }

        def db2start_time = null
        try {
            db2start_time = dataServiceUtils.getStringObservationLatestData(instanceActivity, "db2start_time");
        } catch (e) {
            CAT.error("DB2 - createDB2InstanceData(): Exception happens when call \"instanceActivity.db2start_time\". dbInstance:${DB2_Instance_toString(dbInstance)}.", e)
        }
        //get the upSince value
        def upSince = null;
        if (db2start_time!=null){
            upSince = new Date(Long.parseLong(db2start_time));
        }
        if (TRACE_ENABLED) {
            CAT.trace("DB2 - createDB2InstanceData(): dbVersion:${dbVersion}, db2start_time:${db2start_time}, upSince:${upSince}.")
        }


        //get the OS metrics
        def cpu 	= null;
        def memory 	= null;
        def disk 	= null;
        def osGeneral = null;

        if (dbInstance.get("hosts")!=null){
            def insHosts = dbInstance.get("hosts");
            def coordinatorHost = null;
            for (host in insHosts){
                if (host.getName().equals(hostName)){
                    coordinatorHost = host;
                }
            }
            if (coordinatorHost!=null){
                if (coordinatorHost.get("OS_general")!=null){
                    def os_general = coordinatorHost.get("OS_general");
                    if (os_general.get("system_cpu_utilization")!=null)
                        cpu = os_general.get("system_cpu_utilization");
                    if (os_general.get("usedrampct")!=null)
                        memory = os_general.get("usedrampct");
                    if (os_general.get("disk_utilization")!=null)
                        disk = os_general.get("disk_utilization");
                }
            }
        }

        // set OS monitoring state
        def osMonitoringState = getDB2OSMonitoringState(monitoringAgent, dbInstance.get("monitoredHost"), cpu);

        //creating a list of topologies which relevant for alarms proposal
        def topologiesForAlarms = [];
        topologiesForAlarms.add(dbInstance); 				//DB2_Instance
        topologiesForAlarms.add(monitoringAgent);		//DB2_Agent_Model

        //get the partitions number
        //Replace the host name in case it partitioned or PureScale
        hostName = replaceDB2HostnameIfNeeded(dbInstance, hostName);
        def host = null;
        if (!(hostName.equals("PureScale") || hostName.equals("Partitioned"))) {
            host = dbInstance.get("monitoredHost");
        }

        def paStatus = "-1"; 			    //not supported for DB2
        def port = 0;					    //not supported for DB2
        def isEnableVMCollections = false;	//not supported for DB2

        if (TRACE_ENABLED) {
            CAT.trace("DB2 - createDB2InstanceData(): Before call fillInstanceData(...). hostName:${hostName}.")
        }
        fillInstanceData(dbInstance, monitoringAgent, dbVersion, upSince, hostName, instanceName, port, clusterIdentifier, clusterData,
                false, 0, topologiesForAlarms, cpu, memory, disk, paStatus,
                isEnableVMCollections, osMonitoringState,null,hostTopologysForAlarms, host);
        clusterData.get("instanceIdentifier").set("alias", db2AgentName);
        if (TRACE_ENABLED) {
            CAT.trace("DB2 - createDB2InstanceData(): After call fillInstanceData(...) and set clusterData.instanceIdentifier.alias=${db2AgentName}.")
        }

        //set the workload metric
        try{
            //set the workload
            def workload = dbInstance.get("agents_summary")?.get("active_rate", specificTimeRange);
            clusterData.set("Workload", workload);
        } catch (Exception e) {
            CAT.error("DB2 - createDB2InstanceData(): Exception happens when call \"dbInstance.agents_summary.active_rate\". ${paramsStr}", e)
        }
    } else {
        // fill data for null instance where only the agent topology exists
        def topologiesForAlarms = []
        topologiesForAlarms.add(monitoringAgent);
        if (TRACE_ENABLED) {
            CAT.trace("DB2 - createDB2InstanceData(): else - Before call fillInstanceData(...).")
        }
        fillInstanceData(monitoringAgent, monitoringAgent, null, null, null, null, null, clusterIdentifier, clusterData, false, 0, topologiesForAlarms, null, null, null, null, null, null,null,hostTopologysForAlarms,null);
        clusterData.set("isDataExist" , false);
    }
    if (TRACE_ENABLED) {
        CAT.trace("DB2 - createDB2InstanceData(): exit.")
    }
}


def buildParamsStrForCreateDB2DatabaseData (currDBIns, instanceData, db2InstanceData, db2Instance) {
    def paramsStr = new StringBuilder()
    paramsStr << "paramsStr: [\n currDBIns:${DB2_Database_ToString(currDBIns)}\n, instanceData:${DBWC_GV_GVClusterData_ToString(instanceData)}\n, "
    paramsStr << "db2InstanceData:${DBWC_GV_GVInstanceData_ToString(db2InstanceData)}\n, db2Instance:${DB2_Instance_toString(db2Instance)}\n]"
    return paramsStr.toString()
}
/**
 * This function creates the data of the DB2 instances with all of his databases instances beneath it.

 Inputs:
 currDBIns 	 			of type DB2_Database
 instanceData 			DataObject of type DBWC_GV_GVClusterData
 db2InstanceData  		DataObject of type DBWC_GV_GVInstanceData
 db2Instance 	 	    of type DB2_Instance
 **/
def createDB2DatabaseData (currDBIns, instanceData, db2InstanceData, db2Instance) {
    if (TRACE_ENABLED) {
        def paramsStr = buildParamsStrForCreateDB2DatabaseData(currDBIns, instanceData, db2InstanceData, db2Instance)
        CAT.trace("DB2 - createDB2DatabaseData(): enter. ${paramsStr}")
    }

    def monitoringAgent = currDBIns.get("monitoringAgent");
    def clusterData = instanceData;
    if (clusterData == null){
        clusterData = createDataObject("DBWC_GV_GVClusterData");
    }
    // Identifier
    def db2AgentName = currDBIns.getName();

    def clusterIdentifier;
    if (clusterData.instanceIdentifier != null) {
        clusterIdentifier = updateClusterIdentifier(db2AgentName, clusterData.instanceIdentifier.cluster);
    } else {
        def db2DBType = gDBTypeMap.get("DB_DB2");
        clusterIdentifier = createClusterIdentifier(db2AgentName, db2DBType, false);
    }

    clusterData.set("clusterIdentifier", null);
    db2InstanceData.set("name", currDBIns.getName());

    //getting the collections for the used data in the GV table.
    def instanceActivity = db2Instance.get("instance_activity");
    def dbGeneralActivity = currDBIns.get("general_activity");

    /*
    if (instanceActivity==null){
        def msg = "DB2 database with name: " + db2AgentName + " of " +db2Instance.getName() + " DB_Instance, his Instance Activity collection is null and could not be displayed in Global view";
        printDebugModeLog (msg, null);
        return false;
    }
    /* DDB-3877
    if (dbGeneralActivity==null){
        def msg = "DB2 database with name: " + db2AgentName + " of " +db2Instance.getName() + " DB_Instance, his General Activity collection is null and could not be displayed in Global view";
        printDebugModeLog (msg, null);
        return false;
    }
    */

    def hostName = db2Instance.get("monitoredHost").getName();
    def dbVersion = "";
    try {
        dbVersion = instanceActivity != null ? dataServiceUtils.getStringObservationLatestData(instanceActivity, "product_name") : null ;
    } catch (e) {
        CAT.error("DB2 - createDB2DatabaseData(): Exception happens when call \"instanceActivity.product_name\". db2Instance:${DB2_Instance_toString(db2Instance)}.", e)
    }


    //get the upSince value
    def db2start_time = dbGeneralActivity != null ? dataServiceUtils.getStringObservationLatestData(dbGeneralActivity, "db_conn_time") : null;
    def dbUsability = currDBIns.get("database_usability");

    if (dbUsability != null) {
        def hadr_db_role = dataService.retrieveLatestValue(dbUsability, "hadr_db_role")?.value;
        if (hadr_db_role != null && hadr_db_role.equals("STANDBY")) {
            db2start_time = "-1";
        }
    }

    if (TRACE_ENABLED) {
        CAT.trace("DB2 - createDB2DatabaseData(): before upSince. dbVersion:${dbVersion}, db2start_time:${db2start_time}, dbUsability:${dbUsability}.")
    }
    def upSince = null;
    if (db2start_time!=null){
        upSince = new Date(Long.parseLong(db2start_time));
    }

    def instanceName = currDBIns.get("db_name");
    def paStatus = "-1"; 			//not supported for DB2 Ararat
    //get the OS metrics
    def cpu 	= null;
    def memory 	= null;
    def disk 	= null;

    if (db2Instance.get("hosts")!=null){
        def insHosts = db2Instance.get("hosts");
        def coordinatorHost = null;
        for (host in insHosts){
            if (host.getName().equals(hostName)){
                coordinatorHost = host;
            }
        }
        if (coordinatorHost!=null){
            if (coordinatorHost.get("OS_general")!=null){
                def os_general = coordinatorHost.get("OS_general");
                if (os_general.get("system_cpu_utilization")!=null)
                    cpu = os_general.get("system_cpu_utilization");
                if (os_general.get("usedrampct")!=null)
                    memory = os_general.get("usedrampct");
                if (os_general.get("disk_utilization")!=null)
                    disk = os_general.get("disk_utilization");
            }
        }
    }

    // set OS monitoring state
    def osMonitoringState = getDB2OSMonitoringState(monitoringAgent, db2Instance.get("monitoredHost"), cpu);
    hostName = replaceDB2HostnameIfNeeded(db2Instance, hostName);

    def host = null;
    if (!(hostName.equals("PureScale") || hostName.equals("Partitioned"))) {
        host = db2Instance.get("monitoredHost");
    }

    def topologiesForAlarms = []
    topologiesForAlarms.add(currDBIns);	//DB2_Database
    topologiesForAlarms.add(currDBIns.get("database_usability")); //DB2_Database_Usability


    def isEnableVMCollections = false;	//not supported for DB2 Ararat
    def port = 0;


    if (TRACE_ENABLED) {
        CAT.trace("DB2 - createDB2DatabaseData(): before call fillInstanceData(...). osMonitoringState:${osMonitoringState}, hostName:${hostName}.")
    }
    fillInstanceData(currDBIns,
            monitoringAgent,
            dbVersion,
            upSince,
            hostName,
            instanceName,
            port,
            clusterIdentifier,
            db2InstanceData,
            false,
            0,
            topologiesForAlarms,
            cpu,
            memory,
            disk,
            paStatus,
            isEnableVMCollections,
            osMonitoringState,
            null,
            null,
            host);
    if (TRACE_ENABLED) {
        CAT.trace("DB2 - createDB2DatabaseData(): after call fillInstanceData(...).")
    }

    try{
        //set the workload
        def workload = currDBIns.get("agents_summary")?.get("active_rate", specificTimeRange);
        db2InstanceData.set("Workload", workload);
    } catch (Exception e) {
        CAT.error("DB2 - createDB2DatabaseData(): Exception when get currDBIns.agents_summary.active_rate. db2Instance:${DB2_Instance_toString(db2Instance)}.", e)
    }

    // check if the database is marked to be monitored by the agent in the ASP
    def isDBMonitored = false;
    def dbASPs = monitoringAgent?.get("asp_config")?.get("monitored_databases");
    if ((dbASPs != null) && (dbASPs.size() > 0)) {
        for (dbASP in dbASPs) {
            if (currDBIns.getName().equalsIgnoreCase(dbASP.get("database_name"))) {
                def isMonitoredValue = dataServiceUtils.getStringObservationLatestData(dbASP, "is_monitored");
                isDBMonitored = ((isMonitoredValue != null) && (isMonitoredValue.equalsIgnoreCase("true")));
                break;
            }
        }
    }
    if (!isDBMonitored) {
        db2InstanceData.set("agentState", "1.0");
    }

    if (TRACE_ENABLED) {
        CAT.trace("DB2 - createDB2DatabaseData(): exit.")
    }
    //return 'true' value for finish creating this function successfully
    return true;
}

/**
 * This function creates the data fulfillment of Azure instance
 */
def createMssqlAzureInstanceData (sqlAzureInstance, GVclusterData) {

    def clusterData = GVclusterData;
    if (clusterData == null){
        clusterData = createDataObject("DBWC_GV_GVClusterData");
    }
    // Identifier
    def monitoringAgent = sqlAzureInstance.get("monitoringAgent");
    def sqlAgentName = "";
    def azureDBtype = gDBTypeMap.get("AZURE");

    //updating or creating the instanceIdentifier
    def clusterIdentifier;
    if (clusterData.instanceIdentifier != null) {
        clusterIdentifier = updateClusterIdentifier(sqlAgentName, clusterData.instanceIdentifier.cluster);
    } else {
        clusterIdentifier = createClusterIdentifier(sqlAgentName, azureDBtype, false);
    }
    clusterData.set("clusterIdentifier", clusterIdentifier);
    clusterData.setName(monitoringAgent.get("name"));

    //table fields
    def instanceName = sqlAzureInstance.getName();
    def port = 0;
    def dbVersion = "unknown";
    def upSince = new Date(-1);
    def hostName = "SQL Azure Instance";
    def cpu 	= null;				    //not supported on Azure
    def memory 	= null;				    //not supported on Azure
    def disk 	= null;				    //not supported on Azure
    def paStatus = "-1"; 			    //not supported on Azure
    def isEnableVMCollections = false;	//not supported on Azure

    //get the usability
    def sqlUsability = sqlAzureInstance.get("monitoringAgent" , specificTimeRange).get("usability" , specificTimeRange);
    sqlAgentName = sqlAzureInstance.getName();
    if (sqlUsability != null) {

        if (sqlAzureInstance.get("instance_information")!=null){
            if (sqlAzureInstance.get("instance_information").get("product_version")!=null){
                dbVersion = sqlAzureInstance.get("instance_information").get("product_version");
            }
        }

        if (sqlUsability.get("up_since")!=null){
            upSince = sqlUsability.get("up_since");
        }
        else {
            upSince = new Date(-1);
        }
    }
    else {
        printDebugModeLog ("Global View - Missing usability for " + monitoringAgent.get("name"), null);
    }

    // set OS monitoring state
    def osMonitoringState = gOSMonitoringState.get("Ignore");

    //Alarms list topologies
    def topologiesForAlarms = []
    topologiesForAlarms.add(monitoringAgent); 	//Azure_Instance_Agent
    topologiesForAlarms.add(sqlAzureInstance);	//DBSS_Azure_Instance
    //adding the databases topologies
    for (azureDB in sqlAzureInstance.get("databases")) {
        if (azureDB.get("monitoringAgent")!=null){
            topologiesForAlarms.add(azureDB.get("monitoringAgent")); 		//Azure_Database_Agent
        }
    }


    //filling the Azure instance topology with the data
    fillInstanceData(sqlAzureInstance,
            monitoringAgent,
            dbVersion,
            upSince,
            hostName,
            instanceName,
            port,
            clusterIdentifier,
            clusterData,
            false,
            0,
            topologiesForAlarms,
            cpu,
            memory,
            disk,
            paStatus,
            isEnableVMCollections,
            osMonitoringState,
            null,
            null,
            null); //empty host for SQL Azure


    clusterData.get("instanceIdentifier").set("alias", sqlAgentName);

    try {
        instanceConnectionSummary = sqlAzureInstance.get("azure_connections_summary").get("instance_connections_summary");
        clusterData.set("Workload",instanceConnectionSummary.get("all_active_connections_cnt", specificTimeRange));
    } catch (Exception e) {
        printDebugModeLog ("Error in MSSQL Azure get azure_connections_summary/instance_connections_summary/all_active_connections_cnt",e)
    }

    return clusterData;
}


/**
 * This function creates the data fulfillment of a sol Azure database
 * Params:
 currDBIns - DBSS_Azure_Database
 instanceData - DBWC_GV_GVClusterData
 azureInstanceData - DBWC_GV_GVClusterData
 azureDBInstanceParent - DBSS_Azure_Instance
 azureDBType - DBWC_GV_DatabaseTypeEnum
 azureInstName - String
 **/
def createMssqlAzureDatabaseData (currDBIns, instanceData, azureInstanceData, azureDBInstanceParent, azureDBType, azureInstName){

    def monitoringAgent = currDBIns.get("monitoringAgent");
    def _clusterData = instanceData;
    if (_clusterData == null){
        _clusterData = createDataObject("DBWC_GV_GVClusterData");
    }
    // Identifier
    def sqlAgentName = currDBIns.getName();

    def clusterIdentifier;
    if (_clusterData.instanceIdentifier != null) {
        clusterIdentifier = updateClusterIdentifier(sqlAgentName, _clusterData.instanceIdentifier.cluster);
    } else {
        clusterIdentifier = createClusterIdentifier(sqlAgentName, azureDBType, false);
    }

    _clusterData.set("clusterIdentifier", null);
    _clusterData.setName(monitoringAgent.get("name"));

    def dbVersion = "unknown";
    def sqlAzrueUsability = monitoringAgent.get("usability" , specificTimeRange);

    def upSince = null;


    if (sqlAzrueUsability != null) {

        if (azureDBInstanceParent.get("instance_information")!=null){
            if (azureDBInstanceParent.get("instance_information").get("product_version")!=null){
                dbVersion = azureDBInstanceParent.get("instance_information").get("product_version");
            }
        }

        if (sqlAzrueUsability.get("up_since")!=null){
            upSince = sqlAzrueUsability.get("up_since");
        }
        else {
            upSince = new Date(-1);
        }
    }
    else {
        printDebugModeLog ("Global View - Missing usability for " + monitoringAgent.get("name"), null);
    }

    //table fields
    def instanceName = azureInstName + " - " + currDBIns.("name");
    azureInstanceData.set("name", instanceName);
    def port = 0;
    def hostName = azureInstName;
    def cpu 	= null;				//not supported on Azure
    def memory 	= null;				//not supported on Azure
    def disk 	= null;				//not supported on Azure
    def paStatus = "-1"; 			//not supported on Azure
    def isEnableVMCollections = false;	//not supported on Azure

    // set OS monitoring state
    def osMonitoringState = gOSMonitoringState.get("Ignore");

    def topologiesForAlarms = []
    topologiesForAlarms.add(monitoringAgent); 													//Azure_Database_Agent
    topologiesForAlarms.add(currDBIns);															//DBSS_Azure_Database
    topologiesForAlarms.add(currDBIns.get("database_storage", specificTimeRange));				//DBSS_Azure_Storage
    topologiesForAlarms.add(currDBIns.get("azure_connections_summary", specificTimeRange));		//DBSS_Azure_Connections_Summary

    //adding DBSS_Azure_Bandwidth_Summary topology
    if (azureDBInstanceParent.get("bandwidth_usage")!=null){
        def dbBandWidth = azureDBInstanceParent.get("bandwidth_usage");
        //getting the first value from the query because we get only one topology by this query
        def dbBandwidthTopology = functionHelper.invokeQuery("system:dbwc_globalview_quick20views_azure20database20quick20view.48", sqlAgentName, dbBandWidth)[0];
        topologiesForAlarms.add(dbBandwidthTopology);	//DBSS_Azure_Bandwidth_Summary
    }

    fillInstanceData(currDBIns, monitoringAgent, dbVersion, upSince, hostName, instanceName, port, clusterIdentifier, azureInstanceData,
            false, 0, topologiesForAlarms, cpu, memory, disk, paStatus, isEnableVMCollections, osMonitoringState, null,null, null);

    try{
        //set the workload
        def workload = currDBIns.get("azure_connections_summary").get("all_active_connections_cnt", specificTimeRange)
        azureInstanceData.set("Workload", workload);
    } catch (Exception e) {
        printDebugModeLog ("Error in MSSQL Azure Database: get azure_connections_summary/all_active_connections_cnt",e)
    }

    return _clusterData;
}

/**
 * This function creates the data of the Azure instances with all of his databases instances beneath it.
 * The function will display the monitored Azure instance despite that he doesn't have any monitored databases.
 **/

def createMssqlAzureClusterData (sqlAzureInstance, clusterData){


    if (clusterData == null){
        clusterData = createDataObject("DBWC_GV_GVClusterData");
    }
    def azureDBType = gDBTypeMap.get("AZURE");

    def clusterName = sqlAzureInstance.get("name");
    def clusterInstances = clusterData.get("instances");

    def instanceData = null;

    int i = 0;
    for (azureDB in sqlAzureInstance.get("databases")) {
        if (azureDB.get("monitoringAgent")!=null){
            if (i < clusterInstances.size()) {
                instanceData  = clusterInstances.get(i);
            }
            else {
                instanceData = createDataObject("DBWC_GV_GVInstanceData");
                clusterInstances.add(instanceData);
            }
            //Creating an Azure database node
            createMssqlAzureDatabaseData (azureDB, clusterData, instanceData, sqlAzureInstance, azureDBType, clusterName);
            i++;
        }
        else {
            def msg = "Failed to create Mssql Azure Database Data in function - createMssqlAzureClusterData() for agent name: " + azureDB.getName() + ", monitoring Agent Topology is null";
            printDebugModeLog (msg, null);
        }
    }
    //Creating an Azure instance node
    createMssqlAzureInstanceData (sqlAzureInstance, clusterData);

    return clusterData;
}


def buildParamsStrForCreateDB2ClusterData (db2agent, db2InstanceMap, clusterData){
    def paramsStr = new StringBuilder()
    paramsStr << "paramsStr:[\n db2agent:${Agent_ToString(db2agent)},\n clusterData:${DBWC_GV_GVClusterData_ToString(clusterData)}\n]"
    return paramsStr.toString()
}
/**
 * This function creates the data of the DB2 instances with all of his databases instances beneath it.
 *	Params:
 *	db2agent 			-			DB2_Agent_Model
 *  db2InstanceMap        -           DB2_Instance Map
 *	clusterData			-			DBWC_GV_GVClusterData
 **/
def createDB2ClusterData (db2agent, db2InstanceMap, clusterData){
    if (TRACE_ENABLED) {
        def paramsStr = buildParamsStrForCreateDB2ClusterData(db2agent, db2InstanceMap, clusterData)
        CAT.trace("DB2 - createDB2ClusterData(): enter. ${paramsStr}")
    }

    if (clusterData == null){
        clusterData = createDataObject("DBWC_GV_GVClusterData");
    }

    //databases map to monitor key = db name, value = DB2_Database topology
    //this map will include only the relevant databases to monitor
    def databasesToMonitorMap = [:]
    //list for the DB's names to be monitored
    def monitoredDatabasesNameStringList = [];	//list of type String

    def clusterDatabases = clusterData.get("instances");

    def db2Instance = db2agent.get("instance");
    if (db2Instance != null) {
        if (uniqueClusterMap.get(db2Instance.name) != null)
            return;

        uniqueClusterMap.put(db2Instance.name , db2Instance.name);


        // get the DB2_Instance object from the map as it includes the object data (it came from WCF function)
        db2Instance = db2InstanceMap.get(db2Instance.get("name").toUpperCase());
        if (db2Instance != null) {
            //getting all the DB2_Databases topologies list which are monitored
            for (db in db2Instance.get("databases")) {
                monitoredDatabasesNameStringList.add(db.getName());
                //adding the topology to map
                databasesToMonitorMap.put(db.getName(), db);
            }
        }
    }

    if (TRACE_ENABLED) {
        CAT.trace("DB2 - createDB2ClusterData(): databasesToMonitorMap.size():${databasesToMonitorMap?.size()}.")
    }
    //if we have monitored databases, we need to add them
    if (databasesToMonitorMap.size() > 0) {

        //for each monitored DB, we creating his data for the table.
        for (dbKey in databasesToMonitorMap.keySet()) {
            dbToMonitor = databasesToMonitorMap.get(dbKey);
            def databaseTopologyData = null;
            def isDBTopologyExist = false;

            //getting the relevant database topology to update
            for (cluster in clusterDatabases){
                if (cluster.getName().equalsIgnoreCase(dbToMonitor?.getName())){
                    databaseTopologyData = cluster;
                    isDBTopologyExist = true;
                    break;
                }
            }

            //if we didn't found the topology to update, we creating it
            if (databaseTopologyData == null) {
                databaseTopologyData = createDataObject("DBWC_GV_GVInstanceData");
            }
            def isDBCreated = false;
            try {
                //due to collections that can be throw exception, we putting this function in try catch clause
                isDBCreated = createDB2DatabaseData (dbToMonitor, clusterData, databaseTopologyData, db2Instance);
            }
            catch (Exception e){
                CAT.error("DB2 - createDB2ClusterData(): Failed to create GV Data for DB2 database name:${dbToMonitor?.getName()}, db2agent.name:${db2agent?.get("name")}.", e)
            }

            if (isDBCreated && !isDBTopologyExist){
                clusterDatabases.add(databaseTopologyData);
            }
        }
    }

    //filtering the databases that don't need to be, due to the ASP change
    def toDisplayMap = [:];
    for (int i=0; i<clusterDatabases.size(); i++){
        if (clusterDatabases.get(i) != null && monitoredDatabasesNameStringList.contains(clusterDatabases.get(i).getName())){
            toDisplayMap.put(clusterDatabases.get(i).getName(), clusterDatabases.get(i));
        }
    }

    def toDisplayList = new ArrayList(toDisplayMap.values());

    //updating the list
    clusterData.set("instances", toDisplayList);
    //Creating a DB2 instance node
    createDB2InstanceData (db2Instance, clusterData, db2agent);

    if (TRACE_ENABLED) {
        CAT.trace("DB2 - createDB2ClusterData(): exit.")
    }
    return clusterData;
}


/**
 * returns the PA status for the instance
 possible returned statuses:
 STR_CONST_SC_DOWN = "3";
 STR_CONST_SC_ERROR = "2";
 STR_CONST_SC_OK = "1";
 STR_CONST_SC_NOT_CONFIGURED = "0";
 **/
def getPAUsabilityStatusForSQL(convertSql, pAgent, agentVersion) {
    // if agent status is Unknown return null
    if (getAgentRunningState(pAgent, null).equals("0.0") ) {
        return null;
    }
    // set the default SPI/PA status for this case
    def paStatus;
    if (UpgradeCheck.isNewerVersion(agentVersion, "5.7.5.9")) {
        paStatus = getSPIStatusForSQL(convertSql, pAgent, "spi_conn_profile", "enable_spi_collections", "spi_usability");
    } else if (UpgradeCheck.isNewerVersion(agentVersion, "5.6.10.9")) {
        paStatus = getSPIStatusForSQL(convertSql, pAgent, "pa_conn_profile", "enable_pa_collections", "pa_usability");
    } else {
        paStatus = getSCStatusForSQL(convertSql, pAgent);
    }
    return paStatus;
}

/**
 * Check if SQL PA is configured
 * @param convertSql instance topology
 * @param pAgent agent topology
 * @return "0" for not configured, "1" for configured, "2" for error
 */
def getSPIStatusForSQL(convertSql, pAgent, connProfilePropName, enableCollectionPropName, usabilityPropName) {
    def paStatus = "10";
    def paConnProfile = pAgent?.get("asp_config")?.get(connProfilePropName);
    if (paConnProfile) {
        def paConfigured = dataServiceUtils.getStringObservationLatestData(paConnProfile, enableCollectionPropName);
        if (paConfigured) {
            try {
                if (Boolean.valueOf(paConfigured as String)) {
                    paStatus = "11";

                    // search for errors
                    def paAlerts = convertSql.get(usabilityPropName)?.get("alerts");
                    if (paAlerts && !paAlerts.isEmpty()) {
                        for (paAlert in paAlerts) {
                            def paAlertLevel = dataService.retrieveLatestValue(paAlert, "alert_level")?.getValue();
                            if (paAlertLevel && (paAlertLevel.index > 0)) {
                                paStatus = "12";
                                break;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                printDebugModeLog (String.format("Global View - getSPIStatusForSQL: Failed to parse %s value: %s", enableCollectionPropName, paConfigured), null);
            }
        }

    }
    return paStatus;
}
/**
 * Check if StealthCollect is configured and working
 */
def getSCStatusForSQL(convertSql, pAgent) {
    def paStatus = "0";
    try {
        if (convertSql.get("pa_usability" , specificTimeRange) != null) {

            def paUsability = convertSql.get("pa_usability" , specificTimeRange);

            def paUsabilityList = []
            paUsabilityList.add(paUsability);

            def paStatusesList = [];
            paStatusesList = getInstancePAStatus(convertSql);

            //calling to dan's function
            paStatus = getSCStatus(paUsabilityList, paStatusesList, "mssql", isSQLServerNewVersion, mssqlMinimalSCVersion); //RETURNS THE STATUS
        }
    }
    catch (Exception e){
        printDebugModeLog ("Global View - getSCStatusForSQL: Missing PA usability topology for " + pAgent.get("name"), null);
    }
    return paStatus;
}

def buildParamsStrForCreateMSSQLClusterData (agentSql, instanceSql, clusterSql, GVclusterData, allHostTopologies) {
    def paramsStr = new StringBuilder()
    paramsStr << "paramsStr: [\n agentSql:${Agent_ToString(agentSql)},\n instanceSql:${topologyBasicToString(instanceSql)},\n "
    paramsStr << "clusterSql:${topologyBasicToString(clusterSql)},\n GVclusterData:${DBWC_GV_GVClusterData_ToString(GVclusterData)},\n allHostTopologies:${allHostTopologies}\n]"
    return paramsStr.toString()
}
/*
	This function creates a topology for single SQL Server instance.
	* Returned data:
		instance name, host,version,  alarms, host metrics, up since, workload, PA status

Parameters:
	# agentSql				DBSS_Agent_Model
	# instanceSql			DBSS_Instance
	# clusterSql			DBSS_Clusters_Data
	# GVclusterData 		DBWC_GV_GVClusterData
	# allHostTopologies 	List of all the Host topologies in the system, used only if we have vFoglight

*/
def createMSSQLClusterData (agentSql, instanceSql, clusterSql, GVclusterData, allHostTopologies) {
    if (TRACE_ENABLED) {
        def paramsStr = buildParamsStrForCreateMSSQLClusterData(agentSql, instanceSql, clusterSql, GVclusterData, allHostTopologies)
        CAT.trace("MSSQL - createMSSQLClusterData(): enter. ${paramsStr}")
    }

    try {
        cartrigeVersion = functionHelper.invokeFunction("system:dbwc.activatedCartridgeVersion","DB_SQL_Server_UI");
        isGreaterCart = functionHelper.invokeFunction("system:dbwc_globalview_spiglobalview.compareVersion",cartrigeVersion,"5.7.1.5");
        def _clusterData = GVclusterData;
        if (_clusterData == null)
            _clusterData = createDataObject("DBWC_GV_GVClusterData");

        def sqlAgentName;
        if (isSQLServerNewVersion || (instanceSql == null)){
            sqlAgentName  = agentSql.get("name");
        }
        else{
            sqlAgentName  = instanceSql.get("name");
        }
        def mssqlDBtype = gDBTypeMap.get("MSSQL");

        def _clusterIdentifier;
        if (_clusterData.instanceIdentifier != null) {
            _clusterIdentifier = updateClusterIdentifier(sqlAgentName, _clusterData.instanceIdentifier.cluster);
        } else {
            _clusterIdentifier = createClusterIdentifier(sqlAgentName, mssqlDBtype, false);
        }
        _clusterData.set("clusterIdentifier", _clusterIdentifier);
        _clusterData.setName(agentSql.get("name"));
        def _port = 0;
        def _dbVersion = "unknown";
        def _upSince = null; //equals to "Instance down"
        //Convert - Object to Instance
        def convertSql = null;
        if (instanceSql != null) {
            convertSql = functionHelper.invokeFunction("system:dbwc_mssql.30", instanceSql);
        }

        def sqlUsability = null;
        try {
            def sqlUsabilityScope = QueryServiceUtils.queryScope(agentSql, "usability", "DBSS_Usability");
            if ((sqlUsabilityScope != null) && (sqlUsabilityScope.size() > 0)) {
                //the first element its the usability
                sqlUsability = sqlUsabilityScope.get(0);
            }
        } catch (Exception ex1) {
            try {
                // try and use the old structure were the usability resided under the instance
                sqlUsability = convertSql?.get("usability" , specificTimeRange);
            } catch (Exception ex2) {
                printDebugModeLog ("Failed to retrieve usability for " + agentSql.get("name"), null);
            }
        }

        if (sqlUsability != null) {
            _dbVersion = sqlUsability.getString("db_version");
            db_startValue = sqlUsability.get("db_start");

            //get the latest value of the metric
            def latestAvailability = dataService.retrieveLatestValue(sqlUsability, "availability");
            if (latestAvailability != null) {
                def DEF_SIX_MINUTES = 6 * 60 * 1000
                def timeInterval = DEF_SIX_MINUTES;
                def collectionObject = convertSql?.get("agent")?.get("usability");
                //println("the collection Object is " + collectionObject);
                def availabilityMetric = collectionObject?.get("availability");
                //println("the availability metric is :" + availabilityMetric);
                def startTime = collectionObject?.get("db_start");
                //println("the dbStart is : " + startTime);
                def status = functionHelper.invokeFunction("system:dbwc_infrastructure_components_charts20and20gauges.GetStatusForAvailabilityTile", availabilityMetric, timeInterval, collectionObject, startTime);
                //println("the status is " + status);
                //def latestAverage = latestAvailability.getValue().getAvg();
                if(status.equals("Up")){
                    _upSince = new Date(startTime);
                } else if(status.equals("Down")){
                    _upSince = new Date(-1);// is status is "Down" or "Unknown"
                } else {
                    _upSince = new Date(0);
                }
            }
        } else {
            printDebugModeLog ("Global View - Missing usability for " + agentSql.get("name"), null);
        }
        def hostTopologysForAlarms = [];
        try{
            if(isGreaterCart >= 0) {
                hostTopologysForAlarms.add(agentSql.get("active_host"));
            }
            else {
                hostTopologysForAlarms.add(agentSql.get("monitoredHost"));
            }
        }catch(e){}
        if (instanceSql != null) {
            def _hostName;
            def host = null;
            if(isGreaterCart >= 0) {
                _hostName = instanceSql.get("active_host")?.name;
                host = instanceSql.get("active_host");
            }
            else {
                _hostName = instanceSql.getString("mon_host_name");
                host = instanceSql.get("monitoredHost");

            }
            def _instanceName = instanceSql.getString("mon_instance_name");

            //PI connection status
            def agentVersion = agentSql.get("agentVersion");
            def paStatus = getPAUsabilityStatusForSQL(convertSql, agentSql, agentVersion);
            if (TRACE_ENABLED) {
                CAT.trace("MSSQL - createMSSQLClusterData(): agentSql:${agentSql?.name}, paStatus:${paStatus}.")
            }

            def cpu 	= null;
            def memory 	= null;
            def disk 	= null;

            def isAgentFail = false;
            def isVMFail 	= false;

            def isMonnitorOS = false;
            try {
                def aspConfig = agentSql.get("asp_config");
                if (aspConfig != null) {
                    isMonnitorOS = dataServiceUtils.getStringObservationLatestData(aspConfig, "is_enable_os_collections").equals("true");
                }
            }catch (e){
                printDebugModeLog("Global View asp_config for agent [" + agentSql.get("name") + "] not exist", null);
            }

            // set OS monitoring state
            def isMonnitorIC;
            if(isGreaterCart >= 0) {
                isMonnitorIC = isHostMonitoredByIC(instanceSql.get("active_host"));
            }
            else {
                isMonnitorIC = isHostMonitoredByIC(instanceSql.get("monitoredHost"));
            }
            def osMonitoringState = getOSMonitoringState(isMonnitorOS, isMonnitorIC);

            //first we trying to get the data from the agent if the agent is monitor the OS
            if ((convertSql.get("sql_server_host") != null) && (isMonnitorOS)){
                //checking if the agent has those metrics for backward compatibility
                if (convertSql.get("sql_server_host")!=null){
                    if (convertSql.get("sql_server_host").get("total_cpu_usage", specificTimeRange)!=null){
                        cpu = convertSql.get("sql_server_host").get("total_cpu_usage", specificTimeRange);
                    }
                }
                if (convertSql.get("sql_server_host")!=null){
                    if (convertSql.get("sql_server_host").get("DBSS_Memory_Utilization", specificTimeRange)!=null){
                        memory = convertSql.get("sql_server_host").get("DBSS_Memory_Utilization", specificTimeRange);
                    }
                }
                /*deprecated
                if (convertSql.get("sql_server_host")!=null){
                    if (convertSql.get("sql_server_host").get("disk_utilization", specificTimeRange)!=null){
                        disk = convertSql.get("sql_server_host").get("disk_utilization", specificTimeRange);
                    }
                }
                */
                //in case if all the metrics are null, means that the agent OS metrics are fail
                if (cpu==null && memory==null && disk==null){
                    isAgentFail = true;
                }
            } else {
                //if 'sql_server_host' property is null, means that the agent OS metrics are fail
                isAgentFail = true;
            }
            osType = instanceSql?.version_info?.nt_version;
            if(osType != null && !(osType.toLowerCase().contains("windows"))){
                //get cpu metric from IC cartridge if os type is linux
                cpu = getOSMetricsViaHostNameFromOSCartridge(agentSql, "cpu", cpu);
            }
            disk = getOSMetricsViaHostNameFromOSCartridge( agentSql, "disk", disk);

            //if collecting data from the agent is failed, we trying to collect data from the relevant monitored vm
            if (isAgentFail && gIsVFoglight){
                try{
                    cpu = getOSMetricsOfCertainVM(instanceSql , "cpu");
                    memory = getOSMetricsOfCertainVM(instanceSql , "memory");
                    disk = getOSMetricsOfCertainVM(instanceSql , "disk");

                    //set a name for those metrics for distinguish between them to regular Foglight OS metric
                    //the name is saved only if is needed - if some of the OS metrics are available.
                    cpu.set("name", "vFoglightOSMetric");
                    memory.set("name", "vFoglightOSMetric");
                    disk.set("name", "vFoglightOSMetric");
                }
                catch (Exception e){
                    printDebugModeLog ("There are no OS metrics for agent " + agentSql.get("name") + " from the monitored VM.", null);
                }
            }

            def topologiesForAlarms = []
            topologiesForAlarms.add( agentSql );
            topologiesForAlarms.add( instanceSql );

            def isEnableVMCollections = false;
            def aspConfig = agentSql.get("asp_config");
            if (aspConfig != null) {
                try {
                    isEnableVMCollections = dataServiceUtils.getStringObservationLatestData(aspConfig, "is_enable_vm_collections").equals("true");
                }catch (e){
                    printDebugModeLog("Global View is_enable_vm_collections for agent [" + agentSql.get("name") + "] not exist", null);
                }
            }

            def isReportingServices = false;
            try {
                isReportingServices = instanceSql.get("reporting_services") != null? true : false;
            } //support GV with oldest version 5.5.8-5.6.x
            catch (Exception e){}

            fillInstanceData(instanceSql, agentSql, _dbVersion, _upSince, _hostName, _instanceName, _port, _clusterIdentifier, _clusterData, false, 0, topologiesForAlarms,
                    cpu, memory, disk, paStatus, isEnableVMCollections,
                    osMonitoringState,isReportingServices,hostTopologysForAlarms, host);

            waitsValues = [];
            //Workload breakdowns
            if (UpgradeCheck.isNewerVersion(agentVersion, "5.6.10.9")) {
                wait_event_category = convertSql.wait_event_category;
                waitsList = ["cpu_usage","wait_cpu","wait_io","wait_memory","wait_network","wait_lock","wait_latch","wait_log","wait_clr","remote_provider","wait_xtp","wait_other"];
                if(wait_event_category != null) {
                    for(wait in waitsList) {
                        //value = getCurrentAverageForMetric(wait_event_category,wait);
                        //value = value != null ? value : 0;
                        //waitsValues.add(value);
                        waitsValues.add(wait_event_category.get(wait,specificTimeRange));
                    }
                }
            }
            _clusterData.set("breakdowns", waitsValues);
        } else {

            // fill data for null instance where only the agent topology exists
            def topologiesForAlarms = []
            topologiesForAlarms.add( agentSql );
            fillInstanceData(agentSql, agentSql, null, null, null, null, null, _clusterIdentifier, _clusterData, false, 0, topologiesForAlarms, null, null, null, null, null, null,null,hostTopologysForAlarms, null);
            _clusterData.set("isDataExist" , false);
        }

        _clusterData.get("instanceIdentifier").set("alias", sqlAgentName);

        if (convertSql != null) {
            try {
                tColl = convertSql.wait_event_category;
                _clusterData.set("Workload",tColl.DBSS_Active_Time_Rate);
            } catch (Exception e) {
                printDebugModeLog ("Error in MSSQL get active time rate ",e);
            }
        }
        if (TRACE_ENABLED) {
            CAT.trace("MSSQL - createMSSQLClusterData(): exit.")
        }
        return _clusterData;
    } catch (Exception e) {
        CAT.error("MSSQL - createMSSQLClusterData(): Failed to create MSSQL data for agent id: ${agentSql?.agentID}, return null. agentSql:${topologyBasicToString(agentSql)}, instanceSql:${topologyBasicToString(instanceSql)}." , e)
        return null;
    }
}

/**
 * This function creates a GV topology for single SSRS database
 * inputs: ssrsDatabase, GVclusterData
 */

def createSsrsClusterData (ssrsDatabase, GVclusterData) {
    try {
        def _clusterData = GVclusterData;
        if (_clusterData == null)
            _clusterData = createDataObject("DBWC_GV_GVClusterData");

        def ssrsName = ssrsDatabase.get("name");
        def monitoringAgent = ssrsDatabase.get("monitoringAgent");
        def ssrsDisplayName = ssrsName;
        def biType = gDBTypeMap.get("SSRS");
        if (_clusterData.instanceIdentifier != null) {
            _clusterIdentifier = updateClusterIdentifier(ssrsDisplayName, _clusterData.instanceIdentifier.cluster);
        } else {
            _clusterIdentifier = createClusterIdentifier(ssrsDisplayName, biType, false);
        }
        _clusterData.set("clusterIdentifier", _clusterIdentifier);
        _clusterData.setName(ssrsDisplayName);
        def _port = 0;
        def _dbVersion = "unknown";
        def _upSince = null; //equals to "-"
        def sqlInstance = ssrsDatabase.get("ssrs_repository_instance");
        def hostTopologysForAlarms = [];
        def topologiesForAlarms = [];
        topologiesForAlarms.add(ssrsDatabase);
        //get the up since
        try{
            def ssrs_summary = ssrsDatabase.get("ssrs_summary",specificTimeRange);
            def ssrs_collection_status = ssrsDatabase.get("ssrs_collection_status",specificTimeRange);
            if (ssrs_collection_status !=  null){
                topologiesForAlarms.addAll(ssrs_collection_status);
            }
            if(ssrs_summary != null){
                serviceInfo = ssrs_summary.get("ssrsServiceInfo",specificTimeRange);
                if(serviceInfo != null){
                    topologiesForAlarms.add(serviceInfo);
                }
                //get the latest value of the metric
                def upSince = dataService.retrieveLatestValue(ssrs_summary, "up_since")?.getValue().toLong();
                _upSince = new Date(upSince); //Date

                def serviceState = dataService.retrieveLatestValue(ssrs_summary, "service_state")?.getValue(); //SQL Server Reporting Services is not running
                if(serviceState.equals("Stopped")) {
                    _upSince = new Date(-1); //Instance down
                }else if (_upSince.getTime() == -1){
                    _upSince = null; //-
                }
            }

            def isToRaiseAlarm = dataService.retrieveLatestValue(ssrsDatabase, "is_to_raise_alarm_on_availability")?.getValue(); // 'SSRS - Unable To Connect To The SQL Server Instance' alarm is fired
            if(isToRaiseAlarm.equals("true")) {
                _upSince = new Date(-20); //Unavailable
            }
        }catch(e){
            printDebugModeLog ("Failed to retrieve the Up Since for " + monitoringAgent.get("name"), null);
        }
        //get the agent usability
        def sqlUsability = null;
        try{
            sqlUsability = monitoringAgent.get("usability" , specificTimeRange);
            if (sqlUsability != null) {
                _dbVersion = sqlUsability.getString("db_version");

            } else {
                printDebugModeLog ("Global View - Missing usability for " + monitoringAgent.get("name"), null);
            }
        }
        catch(e){
            printDebugModeLog ("Failed to retrieve usability for " + monitoringAgent.get("name"), null);
        }
        def _hostName;
        def host = null;
        def cpu 	= null;
        def memory 	= null;
        def disk 	= null;
        def isMonnitorOS = false;
        def isEnableVMCollections = false;
        def isSSRSEnabled = false;
        try {
            isMonnitorOS = dataServiceUtils.getStringObservationLatestData(ssrsDatabase, "is_ssrs_os_enabled").equals("true");
            isSSRSEnabled = dataServiceUtils.getStringObservationLatestData(ssrsDatabase, "is_ssrs_enabled").equals("true");
        }catch (e){
            printDebugModeLog("Missing is_ssrs_os_enabled metric for service" + ssrsDatabase.get("name") + " .", null);
        }
        active_host = ssrsDatabase.get("active_host");
        if( active_host != null){
            host = active_host;
        }
        else{
            host = ssrsDatabase.get("monitoredHost");
        }

        // set OS monitoring state
        def isMonnitorIC = isHostMonitoredByIC(host);
        def osMonitoringState = getOSMonitoringState(isMonnitorOS, isMonnitorIC);
        try{
            if(host != null){
                _hostName = host.name;
                host = functionHelper.invokeFunction("system:dbwc.getHostByID" , host);
                hostTopologysForAlarms.add(host);
            }
        }catch(e){
            println("Failed to retrieve host properties for " +ssrsDatabase.name )
        }
        try{
            if(host != null){
                if (host.get("cpus")!=null){
                    cpu = host.get("cpus").get("utilization", specificTimeRange);
                }
                if (host.get("memory")!=null){
                    memory = host.get("memory").get("utilization", specificTimeRange);
                }
                if (host.get("storage",specificTimeRange)!=null){
                    disk = host.get("storage",specificTimeRange).get("diskUtilization", specificTimeRange);
                }
            }
        }catch(e){
            println("Failed to get the host metrics with error:"+e.getMessage())
        }
        fillInstanceData(ssrsDatabase, monitoringAgent, _dbVersion, _upSince, _hostName, ssrsDisplayName, _port, _clusterIdentifier, _clusterData, false, 0, topologiesForAlarms,
                cpu, memory, disk, -1, isEnableVMCollections, osMonitoringState,false,hostTopologysForAlarms, host);
        _clusterData.set("agent", monitoringAgent);
        _clusterData.set("topologyObject", ssrsDatabase);
        //get the running state:
        def runningState = _clusterData.get("agentState");
        if(runningState.equals("5.0")){
            if(!isSSRSEnabled){
                _clusterData.set("agentState", "1.0");
            }
            else{
                _clusterData.set("agentState", runningState);
            }
        }


        return _clusterData;
    }
    catch (Exception e) {
        println("failed to create an object for SSRS database: " + ssrsDatabase?.get("name") + ". error: " + e.getMessage());
        return null;
    }
}

/**
 * This function creates a GV topology for single SSIS database
 * inputs: ssisDatabase, GVclusterData
 */
def createSsisClusterData (ssisDatabase, GVclusterData) {
    try {
        def _clusterData = GVclusterData;
        if (_clusterData == null)
            _clusterData = createDataObject("DBWC_GV_GVClusterData");
        def ssisName = ssisDatabase.get("name");
        def monitoringAgent = ssisDatabase.get("monitoringAgent");
        def ssisDisplayName = ssisName;
        def biType = gDBTypeMap.get("SSIS");
        if (_clusterData.instanceIdentifier != null) {
            _clusterIdentifier = updateClusterIdentifier(ssisDisplayName, _clusterData.instanceIdentifier.cluster);
        } else {
            _clusterIdentifier = createClusterIdentifier(ssisDisplayName, biType, false);
        }
        _clusterData.set("clusterIdentifier", _clusterIdentifier);
        _clusterData.setName(ssisDisplayName);
        def hostTopologysForAlarms = [];
        def topologiesForAlarms = [];
        topologiesForAlarms.add(ssisDatabase);
        //fill properties:
        def _port = 0;
        def _dbVersion = "unknown";
        def _upSince = null; //equals to "Instance down"
        //get the up since
        try{
            def ssis_summary = ssisDatabase.get("ssis_summary",specificTimeRange);
            def ssis_collection_status = ssisDatabase.get("ssis_collection_status",specificTimeRange);
            if (ssis_collection_status !=  null){
                topologiesForAlarms.addAll(ssis_collection_status);
            }
            if(ssis_summary != null){
                serviceInfo = ssis_summary.get("ssisServiceInfo",specificTimeRange);
                if(serviceInfo != null){
                    topologiesForAlarms.add(serviceInfo);
                }
                def DEF_TWO_MINUTES = 2 * 60 * 1000
                def collectionObject = ssisDatabase;
                //println("the collection object is : " + collectionObject.name);
                def ssis = functionHelper.invokeFunction("system:dbwc_infrastructure_components_charts20and20gauges.GetUpdatedIntegrationService",ssisDatabase);
                //println("the ssis is " + ssis);
                def catalogStatus = ssis.get("catalog_status", specificTimeRange);
                //println("the catalogStatusFrom ssis is : " + catalogStatus);
                //println("the type is : " + ssisDatabase.getType());
                def timeInterval = DEF_TWO_MINUTES;
                def startTime = ssisDatabase.get("db_up_since");
                //println("the start time is : " + startTime);
                def status = functionHelper.invokeFunction("system:dbwc_infrastructure_components_charts20and20gauges.GetStatusForAvailabilityTile", catalogStatus, timeInterval, collectionObject, startTime);
                //println("the status is " + status);
                if(status.equals("Up")){
                    _upSince = new Date(startTime);
                } else if(status.equals("Down")){
                    _upSince = new Date(-10);// is status is "Down" present "Unavailable"
                } else {
                    _upSince = new Date(0); //"unknown"
                }
                //_upSince = new Date(-10)
            }
        }catch(e){
            printDebugModeLog ("Failed to retrieve up since  for " + ssisDatabase.name, null);
        }
        //get the agent usability
        def sqlUsability = null;
        try{

            sqlUsability = monitoringAgent.get("usability" , specificTimeRange);

            if (sqlUsability != null) {
                _dbVersion = sqlUsability.getString("db_version");


            } else {
                printDebugModeLog ("Global View - Missing usability for " + monitoringAgent.get("name"), null);
            }
        }
        catch(e){
            printDebugModeLog ("Failed to retrieve usability for " + monitoringAgent.get("name"), null);
        }
        //fill the host properties
        def _hostName;
        def host = null;
        def isMonnitorOS = false;
        def isEnableVMCollections = false;
        def isSSISEnabled = true;
        try {
            isMonnitorOS = dataServiceUtils.getStringObservationLatestData(ssisDatabase, "is_ssis_os_enabled").equals("true");
            isSSISEnabled = dataServiceUtils.getStringObservationLatestData(ssisDatabase, "is_ssis_enabled").equals("true");
        }catch (e){
            printDebugModeLog("Missing is_ssis_os_enabled metric for service" + ssisDatabase.get("name") + " .", null);
        }
        active_host = ssisDatabase.get("active_host");
        if( active_host != null){
            host = active_host;
        }
        else{
            host = ssisDatabase.get("monitoredHost");
        }


        // set OS monitoring state
        def isMonnitorIC = isHostMonitoredByIC(host);

        def osMonitoringState = getOSMonitoringState(isMonnitorOS, isMonnitorIC);

        try{
            if(host != null){
                _hostName = host.name;
                host = functionHelper.invokeFunction("system:dbwc.getHostByID" , host);
                hostTopologysForAlarms.add(host);
            }
        }catch(e){
            println("Failed to retrieve the fresh host  for " +ssisDatabase.name )
        }
        def cpu 	= null;
        def memory 	= null;
        def disk 	= null;
        //get the OS metrics
        try{
            if(host != null){
                if (host.get("cpus")!=null){
                    cpu = host.get("cpus").get("utilization", specificTimeRange);
                }
                if (host.get("memory")!=null){
                    memory = host.get("memory").get("utilization", specificTimeRange);
                }
                if (host.get("storage",specificTimeRange)!=null){
                    disk = host.get("storage",specificTimeRange).get("diskUtilization", specificTimeRange);

                }
            }
        }catch(e){
            println("Failed to get the host metrics with error:"+e.getMessage())
        }

        fillInstanceData(ssisDatabase, monitoringAgent, _dbVersion, _upSince, _hostName, ssisDisplayName, _port, _clusterIdentifier, _clusterData, false, 0, topologiesForAlarms,
                cpu, memory, disk, -1, isEnableVMCollections, osMonitoringState,false,hostTopologysForAlarms, host);
        _clusterData.set("agent", monitoringAgent);
        _clusterData.set("topologyObject", ssisDatabase);
        //get the running state:
        def runningState = _clusterData.get("agentState");
        if(runningState.equals("5.0")){
            if(!isSSISEnabled){
                _clusterData.set("agentState", "1.0");
            }
            else{
                _clusterData.set("agentState", runningState);
            }
        }


        return _clusterData;
    }
    catch (Exception e) {
        println("failed to create an object for SSIS database: " + ssisDatabase?.get("name") + ". error: " + e.getMessage());
        return null;
    }
}

/**
 * This function creates a GV topology for single SSIS database
 * inputs: ssasAgent, GVclusterData
 */

def createSsasClusterData (ssasAgent, GVclusterData) {
    try {
        def _clusterData = GVclusterData;
        if (_clusterData == null)
            _clusterData = createDataObject("DBWC_GV_GVClusterData");
        def ssasName = ssasAgent.get("name")
        def ssasService = ssasAgent.get("service")
        def biType = gDBTypeMap.get("SSAS");
        def topologiesForAlarms = [];
        topologiesForAlarms.add(ssasAgent);
        def hostTopologysForAlarms = [];
        if (_clusterData.instanceIdentifier != null) {
            _clusterIdentifier = updateClusterIdentifier(ssasName, _clusterData.instanceIdentifier.cluster);
        } else {
            _clusterIdentifier = createClusterIdentifier(ssasName, biType, false);
        }
        _clusterData.set("clusterIdentifier", _clusterIdentifier);
        _clusterData.setName(ssasName);
        //fill properties:
        def _port = 0;
        def _dbVersion = "unknown";
        def _upSince = null; //equals to "Instance down"
        //get the up since, db version
        try{
            if(ssasService != null){
                topologiesForAlarms.add(ssasService);
                osHostService  = ssasService.get("osHostService");
                topologiesForAlarms.add(osHostService);
                def general = ssasService.get("general",specificTimeRange);
                if(general != null){
                    _dbVersion = general.get("ssas_version");
                }
            }

        }catch(e){
            println("Failed to retrieve DB Version  for " + ssasName);
        }
        try{
            if(ssasService != null){
                //get the latest value of the metric
                def infra  = ssasService.get("infra",specificTimeRange);
                if(infra != null){
                    //println("the service is : " + ssasService.name)
                    def ssas = functionHelper.invokeFunction("system:dbwc_infrastructure_components_charts20and20gauges.GetUpdatedSSASService",ssasService);
                    def availabilityCollection = ssas.get("availability",specificTimeRange);
                    //println("the availabilityCollection is " + availabilityCollection);
                    def availabilityMetric = availabilityCollection.get("connectivity")?.get("status");
                    //println("the availabilityMetric is " + availabilityMetric);
                    def timeInterval = 120000;
                    def startTime = dataService.retrieveLatestValue(infra, "up_since")?.getValue().toLong();
                    //println ("the startTime is :" + startTime);
                    def status = functionHelper.invokeFunction("system:dbwc_infrastructure_components_charts20and20gauges.GetStatusForAvailabilityTile", availabilityMetric, timeInterval, availabilityCollection, startTime);
                    //println("the status is " + status);
                    if(status.equals("Up")){
                        _upSince = new Date(startTime);
                    } else if(status.equals("Down")){
                        _upSince = new Date(-1);// is status is "Down" or "Unknown"
                    } else {
                        _upSince = new Date(0);
                    }
                    //println("the up since is : " + _upSince);
                }
            }
        }catch(e){
            println("Failed to retrieve up since  for " + ssasName);
        }

        //fill the host properties
        def _hostName;
        def host = null;
        def isMonnitorOS = false;
        def isEnableVMCollections = false;
        def osMonitoringState = null;
        def cpu 	= null;
        def memory 	= null;
        def disk 	= null;
        try {
            def aspConfig = ssasAgent.get("asp_config");
            if (aspConfig != null) {
                isMonnitorOS = dataServiceUtils.getStringObservationLatestData(aspConfig, "enable_os_collections").equals("true");
            }
        }catch (e){
            println("Global View asp_config for agent [" + ssasAgent.get("name") + "] not exist", null);
        }

        // set OS monitoring state
        def isMonnitorIC = isHostMonitoredByIC(ssasAgent.get("monitoredHost"));

        osMonitoringState = getOSMonitoringState(isMonnitorOS, isMonnitorIC);
        try{
            host = ssasAgent.get("monitoredHost");
            if(host != null){
                _hostName = host.name;
                host = functionHelper.invokeFunction("system:dbwc.getHostByID" , host);
                if (host.get("cpus")!=null){
                    cpu = host.get("cpus").get("utilization", specificTimeRange);
                }
                if (host.get("memory")!=null){
                    memory = host.get("memory").get("utilization", specificTimeRange);
                }
                if (host.get("storage",specificTimeRange)!=null){
                    disk = host.get("storage",specificTimeRange).get("diskUtilization", specificTimeRange);
                }
                hostTopologysForAlarms.add(host);
            }
        }catch(e){
            println("Failed to retrieve host properties for " +ssasAgent.name )
        }

        fillInstanceData(ssasService,ssasAgent, _dbVersion, _upSince, _hostName, ssasName, _port, _clusterIdentifier, _clusterData, false, 0, topologiesForAlarms,
                cpu, memory, disk, -1, isEnableVMCollections, osMonitoringState,false,hostTopologysForAlarms, host);
        if(ssasService == null){
            _clusterData.set("isDataExist" , false);
        }

        _clusterData.set("agent", ssasAgent);
        _clusterData.set("topologyObject", ssasAgent);
        return _clusterData;
    }
    catch (Exception e) {
        println("failed to create an object for SSAS database: " + ssasAgent?.get("name") + ". error: " + e.getMessage());
        return null;
    }
}
/*
	This function is responsible of taking all in the inserted parameters
	and put them in the 'gvInstanceData' object.
*/
def fillmySQLInstanceData(gvInstanceData, aAgent, instance, dbVersion, upsince, hostName, instanceName, port,
                          topologiesForAlarms, mCpu, mMemory, mDisk, osMonitoringState, workload) {
    //Create instance identifier
    def ts = server.TopologyService;
    def _clusterIdentifier;
    def sqlAgentName = aAgent.get("name");
    if (gvInstanceData.instanceIdentifier != null) {
        _clusterIdentifier = updateClusterIdentifier(sqlAgentName, gvInstanceData.instanceIdentifier.cluster);
    } else {
        mysqlDBtype = gDBTypeMap.get("MySQL");
        _clusterIdentifier = createClusterIdentifier(sqlAgentName, mysqlDBtype, false);
    }
    gvInstanceData.set("name",sqlAgentName);
    gvInstanceData.set("clusterIdentifier", _clusterIdentifier);
    def iiType = ts.getType("DBWC_GV_InstanceIdentifier");
    def ii = ts.createAnonymousDataObject(iiType);
    ii.set("cluster", _clusterIdentifier);
    ii.set("alias",sqlAgentName);
    ii.set("hostName",hostName);
    ii.set("InstanceName",instanceName);
    ii.set("port",port);
    ii.set("isAggregated",false);
    gvInstanceData.set("instanceIdentifier", ii);
    if ((dbVersion != null) && (dbVersion.equalsIgnoreCase("unknown"))) {
        dbVersion = "";
    }
    topologiesForAlarms_uid = [];
    for(obj in topologiesForAlarms) {
        try{
            refObj = topologyService.getObject(obj.getUniqueId());
            topologiesForAlarms_uid.add(refObj);
        } catch(e) {}
    }
    //Global
    gvInstanceData.set("databaseVersion", dbVersion);
    gvInstanceData.set("stuffedDBVersion", getZeroStuffedVersion(dbVersion));
    gvInstanceData.set("databaseUpSince", upsince);
    gvInstanceData.set("topologyObject", instance);
    gvInstanceData.set("topologysForAlarms", topologiesForAlarms_uid);
    gvInstanceData.set("agent", aAgent);
    gvInstanceData.set("CPUUtilization", mCpu );
    gvInstanceData.set("MemoryUtilization", mMemory);
    gvInstanceData.set("DiskUtilization" ,  mDisk);
    gvInstanceData.set("osMonitoringState" , osMonitoringState);
    gvInstanceData.set("isDataExist" , true); //default value
    gvInstanceData.set("Workload", workload);
    gvInstanceData.set("paState", -1);
    def agentInstance = null;
    if (aAgent != null) {
        agentInstance = gAllAgentMap.get(String.valueOf(aAgent.get("agentID")));
    }
    def runningState = getAgentRunningState(aAgent, agentInstance);
    gvInstanceData.set("agentState", runningState);
    fillIsUpgradeRequired(gvInstanceData, aAgent);
    // agent running and collecting data (status == OK )
    if (runningState.equals(RUNNING_STATE_COLLECTING_DATA) || runningState.equals(BLACKOUT_STATE_COLLECTING_DATA)) {
        def totalHealth = getSeverityForGlobalViewNode(topologiesForAlarms);
        gvInstanceData.set("totalHealth", gSeverityTypeMap.get(totalHealth));
        gvInstanceData.set("systemHealth", gSeverityTypeMap.get(0));
    }
    else {
        // agent not running   (status ==  problem )
        if (runningState == "0.0" ){
            //putting a severity of n/a
            gvInstanceData.set("totalHealth", gSeverityTypeMap.get(-1));
        }
        else{
            gvInstanceData.set("totalHealth", gSeverityTypeMap.get(4));
        }
        gvInstanceData.set("systemHealth", gSeverityTypeMap.get(-1));
    }
}
/*
This function creates a topology for single mySQL instance.
Parameters:
	# agentSql				//The topology model will be changed in the future, but for now agent and instance are the same type
	# instanceSql
	# GVclusterData 		DBWC_GV_GVClusterData
*/
def createMySQLClusterData (agentSql, instanceSql, GVclusterData) {
    try {
        def gvInstance =  GVclusterData;
        if(gvInstance == null)
            gvInstance = createDataObject("DBWC_GV_GVClusterData");
        instanceSql = functionHelper.invokeFunction("system:dbwc_globalview.returnMysqlServerAgent",instanceSql);
        def _instanceName = instanceSql.get("name");
        def _port = instanceSql.get("Connection_Status/ConnectionPort"); //String
        def _dbVersion = instanceSql.get("Database_Information/MySQL_Version"); //String
        def _upSince = instanceSql.get("Database_Information/StartTime"); //Timestamp
        def host = instanceSql.get("monitoredHost");
        def _hostName = host.get("name"); //String
        def workload = instanceSql.get("Workload"); //Metric
        def cpu 	= null;
        def memory 	= null;
        def disk 	= null;
        // set OS monitoring state
        def isMonnitorIC = isHostMonitoredByIC(instanceSql.get("monitoredHost"));
        def isMonnitorOS = gOSMonitoringState.get("IC");
        def osMonitoringState = getOSMonitoringState(isMonnitorOS, isMonnitorIC);
        //Collect Host data if available
        if (host!=null){
            try{
                cpu = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"cpus","utilization");
                memory = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"memory","utilization");
                disk = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"storage","diskUtilization");
            }
            catch (Exception e){
                printDebugModeLog ("There are no OS metrics for agent " + agentSql.get("name") + " from the monitored host.", null);
            }
        }
        def topologiesForAlarms = []
        //topologiesForAlarms.add( agentSql );
        topologiesForAlarms.add(instanceSql);
        def ts = server.TopologyService;
        fillmySQLInstanceData(gvInstance, agentSql, instanceSql, _dbVersion, _upSince, _hostName, _instanceName, _port, topologiesForAlarms,
                cpu, memory, disk, osMonitoringState, workload);
        return gvInstance;
    } catch (Exception e) {
        println("Failed to create MySQL data for agent id [" + String.valueOf(agentSql?.get("agentID")) + "]. error: " + e.getMessage());
        return null;
    }
}

/**
 * Creates a topology for MongoDB Cluster, ReplicaSet, or standalone MongoServer.
 * @param agent - MongoDBAgent
 * @param topoInstance - Mongo_Cluster or MongoReplicaSet
 * @return clusterData - DBWC_GV_GVClusterData
 */
def createMongoTopLevelData(agent, topoInstance, clusterData) {
    try {
        clusterData = clusterData ?: createDataObject("DBWC_GV_GVClusterData");
        String topoTypeName = topoInstance.getTopologyType().getName();
        if ('Mongo_Cluster'.equals(topoTypeName)) {
            return createMongoClusterData(agent, topoInstance, clusterData);
        } else if ('MongoReplicaSet'.equals(topoTypeName)) {
            return createMongoReplicaSetData(agent, topoInstance, clusterData);
        } else {
            println("Invalid MongoDB server parent type: ${topoTypeName}. Must be one of 'Mongo_Cluster' or 'MongoReplicaSet'.");
            return null;
        }
    } catch (Exception e) {
        println("Failed to create MongoDB cluster data for agent id [${String.valueOf(agent?.get('agentID'))}]. Error: ${e.getMessage()}");
        return null;
    }
}

def createMongoClusterData(agent, topoInstance, clusterData) {
    topoInstance = functionHelper.invokeFunction('system:dbwc_globalview.castMongoCluster', topoInstance);
    String instanceName = "${topoInstance.get('name')} (Cluster)";
    def clusterIdentifier = clusterData.get('instanceIdentifier') != null ?
            updateClusterIdentifier(instanceName, clusterData.instanceIdentifier.cluster) :
            createClusterIdentifier(instanceName, gDBTypeMap.get('MongoDB'), true);
    clusterData.set('clusterIdentifier', clusterIdentifier);
    clusterData.setName(instanceName);

    def existingInstances = clusterData.get('instances');
    def childrenNodes = [];
    topoInstance.mongos.collect(childrenNodes) { server ->
        def nodeData =
                existingInstances?.find { it.get('instanceIdentifier/InstanceName').startsWith(server.get('name')) } ?:
                        createDataObject('DBWC_GV_GVClusterData');
        createMongoServerData(agent, server, nodeData, false, true);
        nodeData
    }
    topoInstance.replicaSets.collect(childrenNodes) { replSet ->
        def nodeData =
                existingInstances?.find { it.get('instanceIdentifier/InstanceName').startsWith(replSet.get('name')) } ?:
                        createDataObject('DBWC_GV_GVClusterData');
        createMongoReplicaSetData(agent, replSet, nodeData);
        nodeData
    }
    clusterData.set('instances', childrenNodes);
    setMongoClusterData(agent, topoInstance, clusterData, instanceName, clusterIdentifier);
    return clusterData;
}

def createMongoReplicaSetData(agent, topoInstance, clusterData) {
    topoInstance = functionHelper.invokeFunction('system:dbwc_globalview.castMongoReplicaSet', topoInstance);
    boolean isStandalone = functionHelper.invokeFunction('system:mongodb_replication.replicaSetIsStandalone', topoInstance);
    String displayTypeName = isStandalone ? 'Standalone' : 'Replica Set';
    if (topoInstance.isConfigSet) {
        displayTypeName = 'Config '+ displayTypeName;
    }
    def childrenServers = getMongoServersFromReplicaSet(topoInstance);
    if (isStandalone && childrenServers.size() == 1) {
        createMongoServerData(agent, childrenServers.get(0), clusterData, true, false);
    } else {
        String instanceName = "${topoInstance.get('name')} ($displayTypeName)";
        def clusterIdentifier = clusterData.get('instanceIdentifier') != null ?
                updateClusterIdentifier(instanceName, clusterData.instanceIdentifier.cluster) :
                createClusterIdentifier(instanceName, gDBTypeMap.get('MongoDB'), true);
        clusterData.set('clusterIdentifier', clusterIdentifier);
        clusterData.setName(instanceName);

        def existingInstances = clusterData.get('instances');
        def newClusterNodes = childrenServers.collect { server ->
            def nodeData =
                    existingInstances?.find { it.get('instanceIdentifier/InstanceName').startsWith(server.get('name')) } ?:
                            createDataObject('DBWC_GV_GVClusterData');
            createMongoServerData(agent, server, nodeData, false, false);
            nodeData
        }
        // Create cluster data.
        clusterData.set('instances', newClusterNodes);
        setMongoClusterData(agent, topoInstance, clusterData, instanceName, clusterIdentifier);
    }
    return clusterData;
}

/**
 * Sets topology data on the MongoDB cluster object of type Mongo_Cluster or MongoReplicaSet.
 * @param agent - MongoDBAgent
 * @param topoInstance - Mongo_Cluster or MongoReplicaSet
 * @return clusterData - DBWC_GV_GVClusterData
 */
def setMongoClusterData(agent, topoInstance, clusterData, instanceName, clusterIdentifier) {
    try {
        def mongoServerChildren = getMongoServersFromMongoDBParent(topoInstance);
        def dbVersion   = getMongoUniformVersion(mongoServerChildren);
        def upSince     = getMongoEarliestStartTime(mongoServerChildren);
        def host        = null;
        def port        = null;
        def hostName    = null;
        def cpu         = null;
        def memory      = null;
        def disk        = null;
        def osMonitoringState = gOSMonitoringState.get('Ignore');   // Set OS monitoring state to unknown.
        def piStatus = '-1';                //not supported on MongoDB
        def isEnableVMCollections = false;  //not supported on MongoDB
        def topologiesForAlarms = [agent, topoInstance];
        fillInstanceData(topoInstance, agent, dbVersion, upSince, hostName,
                instanceName, port, clusterIdentifier, clusterData, false, 0,
                topologiesForAlarms, cpu, memory, disk, piStatus,
                isEnableVMCollections, osMonitoringState, null, null, host);
        try {
            clusterData.set('Workload', topoInstance.get('workload'));
        } catch (Exception e) {
            printDebugModeLog('Error in MongoDB get workload ', e);
        }
        return clusterData;
    } catch (Exception e) {
        println("Failed to set MongoDB cluster data for agent id [${String.valueOf(agent?.get('agentID'))}]. Error: ${e.getMessage()}");
        return null;
    }
}

/**
 * Retrieve the MongoServer children of the provided TopologyObject.
 * @param topoInstance      a TopologyObject of type 'MongoCluster' or 'MongoReplicaSet'
 */
def getMongoServersFromMongoDBParent(topoInstance) {
    def servers = [];
    def topoTypeName = topoInstance.getTopologyType().getName();
    if ('Mongo_Cluster'.equals(topoTypeName)) {
        def cluster = functionHelper.invokeFunction('system:dbwc_globalview.castMongoCluster', topoInstance);
        servers.addAll(cluster.mongos);
        servers.addAll(cluster.configsvr);
        servers.addAll(cluster.mongod);
    } else if ('MongoReplicaSet'.equals(topoTypeName)) {
        def replSet = functionHelper.invokeFunction('system:dbwc_globalview.castMongoReplicaSet', topoInstance);
        servers.addAll(replSet.members);
    }
    return servers;
}

/**
 * Retrieve the MongoServer children of the provided replica set TopologyObject.
 * @param topoInstance      a TopologyObject of type 'MongoCluster' or 'MongoReplicaSet'
 */
def getMongoServersFromReplicaSet(topoInstance) {
    def topoTypeName = topoInstance.getTopologyType().getName();
    if (!'MongoReplicaSet'.equals(topoTypeName)) {
        println("Invalid call to getMongoServersFromReplicaSet with instance of type $topoTypeName");
        return [];
    }
    def replSet = functionHelper.invokeFunction('system:dbwc_globalview.castMongoReplicaSet', topoInstance);
    return replSet.members;
}

/** Returns the earliest start time of the provided MongoServer topology objects. */
def getMongoEarliestStartTime(List mongoServers) {
    mongoServers?.findResults { it?.startTime }
            ?.min();
}

/**
 * Returns the common version of the provided servers or 'unknown' if mulitiple
 * versions are found.
 */
def getMongoUniformVersion(List mongoServers) {
    List versions = mongoServers?.findResults { it?.version }
            ?.unique();
    return versions != null && versions.size() == 1 ?
            versions.get(0) : 'unknown';
}

/**
 * Creates a topology for single MongoDB instance.
 * @param agentSql - MongoDBAgent
 * @param instanceSql - MongoServer
 * @return GVclusterData - DBWC_GV_GVClusterData
 */
def createMongoServerData(agentSql, instanceSql, GVclusterData, isStandalone, isMongos) {
    try {
        def _clusterData = GVclusterData;
        if (_clusterData == null)
            _clusterData = createDataObject("DBWC_GV_GVClusterData");
        instanceSql = functionHelper.invokeFunction("system:dbwc_globalview.covertMongoServer",instanceSql); //Retrieve Fresh MongoServer
        def _instanceName = instanceSql.get("name");
        if (functionHelper.invokeFunction('system:mongodb_common.isArbiter', instanceSql)) {
            _instanceName += ' (Arbiter)';
        }
        if (isStandalone) {
            _instanceName += ' (Standalone)';
        }
        if (isMongos) {
            _instanceName += ' (Query Router)';
        }
        def mongoDbType = gDBTypeMap.get("MongoDB");
        def _clusterIdentifier;
        if (_clusterData.instanceIdentifier != null) {
            _clusterIdentifier = updateClusterIdentifier(_instanceName, _clusterData.instanceIdentifier.cluster);
        } else {
            _clusterIdentifier = createClusterIdentifier(_instanceName, mongoDbType, false);
        }
        _clusterData.set("clusterIdentifier", _clusterIdentifier);
        _clusterData.setName(_instanceName);
        def _port = instanceSql.get("dbPort");
        def _dbVersion = instanceSql.get("version");
        def _upSince = instanceSql.get("startTime");
        def host = instanceSql.get("monitoredHost");
        def _hostName = host.get("name");
        def cpu 	= null;
        def memory 	= null;
        def disk 	= null;
        // set OS monitoring state
        def isMonnitorIC = isHostMonitoredByIC(instanceSql.get("monitoredHost"));
        def isMonnitorOS = gOSMonitoringState.get("IC");
        def osMonitoringState = getOSMonitoringState(isMonnitorOS, isMonnitorIC);
        //Collect Host data if available
        if (host != null){
            try{
                cpu = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"cpus","utilization");
                memory = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"memory","utilization");
                disk = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"storage","diskUtilization");
            }
            catch (Exception e){
                printDebugModeLog ("There are no OS metrics for agent " + agentSql.get("name") + " from the monitored host.", null);
            }
        }
        def piStatus = "-1"; 			//not supported on MongoDB
        def isEnableVMCollections = false;	//not supported on MongoDB
        def topologiesForAlarms = []
        topologiesForAlarms.add( agentSql );
        topologiesForAlarms.add( instanceSql );
        fillInstanceData(instanceSql, agentSql, _dbVersion, _upSince, _hostName, _instanceName, _port, _clusterIdentifier, _clusterData, false, 0, topologiesForAlarms,
                cpu, memory, disk, piStatus, isEnableVMCollections, osMonitoringState, null, null, host);
        try {
            _clusterData.set("Workload",instanceSql.get("workload"));
        } catch (Exception e) {
            printDebugModeLog ("Error in MongoDB get workload ", e);
        }
        return _clusterData;
    } catch (Exception e) {
        println("Failed to create MongoDB data for agent id [" + String.valueOf(agentSql?.get("agentID")) + "]. error: " + e.getMessage());
        return null;
    }
}
/**
 * This function creates a topology for Cassandra Cluster and set their nodes.
 * @param cassandraCluster - CassandraCluster
 * @param cassandraAgent - CassandraAgent
 * @return GVclusterData - DBWC_GV_GVClusterData
 */
def createCassandraClustersData(cassandraAgent, cassandraCluster, GVclusterData) {
    try {
        //Create GVClusterData
        def clusterData = GVclusterData;
        if (clusterData == null){
            clusterData = createDataObject("DBWC_GV_GVClusterData");
        }
        def cassandraDBType = gDBTypeMap.get("Cassandra"); //Get Cassandra type
        def clusterName = cassandraCluster.get("name");
        def clusterIdentifier = createClusterIdentifier(clusterName, cassandraDBType, true); //create cluster identifier
        clusterData.set("clusterIdentifier", clusterIdentifier);
        clusterData.setName(clusterName);
        def existingClusterInstances = clusterData.get("instances");
        def newClusterNodes = [];
        def nodeData;
        //create cassandra nodes
        for (node in cassandraCluster.get("nodes")) {
            nodeData = null;
            // search for the existing instance data
            if (existingClusterInstances != null) {
                for (existingClusterInstance in existingClusterInstances) {
                    if (node.get("name").equals(existingClusterInstance.get("instanceIdentifier/InstanceName"))) {
                        nodeData = existingClusterInstance;
                        break;
                    }
                }
            }
            if (nodeData == null) {
                nodeData = createDataObject("DBWC_GV_GVInstanceData");
            }
            newClusterNodes.add(nodeData);
            try {
                createCassandraNodeData(node, nodeData);
            }
            catch (Exception e) {
                printDebugModeLog ("Failed to create Cassandra node : "  +nodeData.get("name") + "\n",e);
            }
        }
        //Create cluster data
        clusterData.set("instances", newClusterNodes);
        createCassandraClusterData(cassandraAgent, cassandraCluster, clusterData);
        return clusterData;
    } catch (Exception e) {
        println("Failed to create Cassandra clusters data for cluster [" + cassandraCluster.name + "]. error: " + e.getMessage());
        return null;
    }
}
/**
 * This function creates a topology for Cassandra Cluster: aggreagation of the nodes.
 * @param agent - CassandraAgent
 * @param cluster - cassandraCluster
 * @return clusterData - DBWC_GV_GVClusterData
 */
def createCassandraClusterData(agent, cluster, clusterData) {
    try {
        cluster = functionHelper.invokeFunction("system:dbwc_globalview.covertCassandraCluster",cluster); //Convert topologyObject to CassandraCluster
        def nodes = cluster.nodes;
        def cassandraNode = nodes[0]; //getting the first node to get values which are the same in the all nodes
        def clusterName = cluster.get("name");
        def dbType = gDBTypeMap.get("Cassandra");
        //Identifier
        def clusterIdentifier = null;
        if (clusterData.get("instanceIdentifier") != null) {
            clusterIdentifier = updateClusterIdentifier (clusterName, clusterData.instanceIdentifier.cluster);
        } else {
            clusterIdentifier = createClusterIdentifier(clusterName, dbType, true);
        }
        def port = cassandraNode.get("port");
        def dbVersion = cassandraNode.get("cassandraVersion");
        //Get the min nodes upSince
        def earlierUpSince = cassandraNode.get("jvm/runtime/startTime");
        for(node in nodes) {
            upSinceNode = node.get("jvm/runtime/startTime");
            if(upSinceNode?.getTime() < earlierUpSince?.getTime()) {
                earlierUpSince = upSinceNode;
            }
        }
        def host = null;
        def cpu 	= null;
        def memory 	= null;
        def disk 	= null;
        def osMonitoringState = gOSMonitoringState.get("Ignore"); // set OS monitoring state to unknown
        def piStatus = "-1"; 			//not supported on Cassandra
        def isEnableVMCollections = false;	//not supported on Cassandra
        def topologiesForAlarms = []
        topologiesForAlarms.add( agent );
        topologiesForAlarms.add( cluster );
        fillInstanceData(cluster, agent, dbVersion, earlierUpSince, "", clusterName, port, clusterIdentifier, clusterData, false, 0, topologiesForAlarms,
                cpu, memory, disk, piStatus, isEnableVMCollections, osMonitoringState, null, null, host);
        try {
            clusterData.set("Workload",cluster.get("workload"));
        } catch (Exception e) {
            printDebugModeLog ("Error in Cassandra get workload ", e);
        }
        return clusterData;
    } catch (Exception e) {
        println("Failed to create Cassandra cluster data for agent id [" + String.valueOf(agent?.get("agentID")) + "]. error: " + e.getMessage());
        return null;
    }
}
/**
 * This function creates a topology for Cassandra Node.
 * @param agent - CassandraAgent
 * @param cassandraNode - CassandraNode
 * @return nodeData - DBWC_GV_GVInstanceData
 */
def createCassandraNodeData(cassandraNode, nodeData) {
    try {
        cassandraNode = functionHelper.invokeFunction("system:dbwc_globalview.covertCassandraNode",cassandraNode); //Convert topologyObject to CassandraNode
        def nodeName = cassandraNode.get("name");
        def dbType = gDBTypeMap.get("Cassandra");
        //Instance identifier
        def nodeUniqueName = cassandraNode.cluster.name+"-"+nodeName;
        nodeData.setName(nodeUniqueName);
        def instanceIdentifier = null;
        if (nodeData.get("instanceIdentifier") != null) {
            instanceIdentifier = updateClusterIdentifier (nodeUniqueName, nodeData.instanceIdentifier.cluster);
        } else {
            instanceIdentifier = createClusterIdentifier(nodeUniqueName, dbType, false);
        }
        def port = cassandraNode.get("port");
        def dbVersion = cassandraNode.get("cassandraVersion");
        def upSince = cassandraNode.get("jvm/runtime/startTime");
        def host = cassandraNode.get("monitoredHost");
        def hostName = host.get("name");
        def cpu 	= null;
        def memory 	= null;
        def disk 	= null;
        // set OS monitoring state
        def isMonnitorIC = isHostMonitoredByIC(cassandraNode.get("monitoredHost"));
        def isMonnitorOS = gOSMonitoringState.get("IC");
        def osMonitoringState = getOSMonitoringState(isMonnitorOS, isMonnitorIC);
        def agent = cassandraNode.monitoringAgent
        //Collect Host data if available
        if (host != null){
            try{
                cpu = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"cpus","utilization");
                memory = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"memory","utilization");
                disk = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"storage","diskUtilization");
            }
            catch (Exception e){
                printDebugModeLog ("There are no OS metrics for agent " + agent.get("name") + " from the monitored host.", null);
            }
        }
        def piStatus = "-1"; 			//not supported on Cassandra
        def isEnableVMCollections = false;	//not supported on Cassandra
        def topologiesForAlarms = []
        topologiesForAlarms.add( agent );
        topologiesForAlarms.add( cassandraNode );
        fillInstanceData(cassandraNode, agent, dbVersion, upSince, hostName, nodeName, port, instanceIdentifier, nodeData, false, 0, topologiesForAlarms,
                cpu, memory, disk, piStatus, isEnableVMCollections, osMonitoringState, null, null, host);
        nodeData.get("instanceIdentifier").set("alias", nodeName);
        try {
            nodeData.set("Workload",cassandraNode.get("workload"));
        } catch (Exception e) {
            printDebugModeLog ("Error in Cassandra get workload ", e);
        }
        return nodeData;
    } catch (Exception e) {
        println("Failed to create Cassandra node data for node " + nodeName + " of cluster " + cassandraNode.cluster?.name + ". error: " + e.getMessage());
        return null;
    }
}

/*
This function creates a topology for single postgreSQL instance.
Parameters:
	# agentSql				PostgreSQLAgent		//The topology model will be changed in the future, but for now agent and instance are the same type
	# instanceSql			PostgreSQLAgent

*/
def createPostgreSQLData (agentSql, instanceSql, GVclusterData) {
    try {
        def _clusterData = GVclusterData;
        if (_clusterData == null)
            _clusterData = createDataObject("DBWC_GV_GVClusterData");

        //Retrieve Fresh PostgreSQL Agent
        instanceSql = functionHelper.invokeFunction("system:dbwc_globalview.RetrieveFreshPostgreSQLAgent", instanceSql);

        def _instanceName = instanceSql.get("name");
        def postgreSQLType = gDBTypeMap.get("PostgreSQL");
        def _clusterIdentifier;
        if (_clusterData.instanceIdentifier != null) {
            _clusterIdentifier = updateClusterIdentifier(_instanceName, _clusterData.instanceIdentifier.cluster);
        } else {
            _clusterIdentifier = createClusterIdentifier(_instanceName, postgreSQLType, false);
        }
        _clusterData.set("clusterIdentifier", _clusterIdentifier);
        _clusterData.setName(_instanceName);

        def _port = instanceSql.get("port"); //String
        def _dbVersion = instanceSql.get("server_version"); //String
        def _upSince = instanceSql.get("start_time"); //Timestamp

        def host = instanceSql.get("monitoredHost");
        def _hostName = host.get("name"); //String
        def cpu 	= null;
        def memory 	= null;
        def disk 	= null;

        // set OS monitoring state
        def isMonnitorIC = isHostMonitoredByIC(instanceSql.get("monitoredHost"));
        def isMonnitorOS = gOSMonitoringState.get("IC");
        def osMonitoringState = getOSMonitoringState(isMonnitorOS, isMonnitorIC);

        //Collect Host data if available
        if (host!=null){
            try{
                cpu = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"cpus","utilization");
                memory = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"memory","utilization");
                disk = functionHelper.invokeFunction("system:dbwc_globalview.getSystemUtilizationMetric",host,"storage","diskUtilization");
            }
            catch (Exception e){
                printDebugModeLog ("There are no OS metrics for agent " + agentSql.get("name") + " from the monitored host.", null);
            }
        }

        def paStatus = "-1"; 			//not supported on postgreSQL
        def isEnableVMCollections = false;	//not supported on postgreSQL

        def topologiesForAlarms = []
        //topologiesForAlarms.add( agentSql );
        topologiesForAlarms.add( instanceSql );


        fillInstanceData(instanceSql, agentSql, _dbVersion, _upSince, _hostName, _instanceName, _port, _clusterIdentifier, _clusterData, false, 0, topologiesForAlarms,
                cpu, memory, disk, paStatus, isEnableVMCollections, osMonitoringState, null, null, host);

        try {
            _clusterData.set("Workload",instanceSql.get("workload"));
        } catch (Exception e) {
            printDebugModeLog ("Error in PostgreSQL get workload ", e);
        }


        return _clusterData;

    } catch (Exception e) {
        println("Failed to create PostgreSQL data for agent id [" + String.valueOf(agentSql?.get("agentID")) + "]. error: " + e.getMessage());
        return null;
    }
}

/**
 * returns the PA status for the instance
 possible returned statuses:
 STR_CONST_SC_DOWN = "3";
 STR_CONST_SC_ERROR = "2";
 STR_CONST_SC_OK = "1";
 STR_CONST_SC_NOT_CONFIGURED = "0";
 **/
def getPAUsabilityStatusForOracle(convertOra, pAgent, isSPI){
    // if agent status is Unknown return null
    if (getAgentRunningState(pAgent, null).equals("0.0") ) {
        return null;
    }

    // set the default PA status for this case
    def paStatus;
    if (isSPI) {
        paStatus = getSPIStatusForOracle(pAgent);
    } else {
        paStatus = getSCStatusForOracle(convertOra, pAgent);
    }
    return paStatus;
}

/**
 * Check if SQL PI is configured
 * @param pAgent agent topology
 * @return "10" for not configured, "11" for configured, "12" for error
 */
def getSPIStatusForOracle(pAgent) {
    def spiStatus = "10";
    def spiConnProfile = pAgent?.get("asp_config")?.get("spi_config");
    if (spiConnProfile) {
        def spiConfigured = dataServiceUtils.getStringObservationLatestData(spiConnProfile, "enable_spi_collections");
        if (spiConfigured) {
            try {
                if (Boolean.valueOf(spiConfigured as String)) {
                    spiStatus = "11";

                    // search for errors
                    def spiAlerts = pAgent.get("usability")?.get("spi_alerts");
                    if (spiAlerts && !spiAlerts.isEmpty()) {
                        for (spiAlert in spiAlerts) {
                            def spiAlertLevel = dataService.retrieveLatestValue(spiAlert, "alert_level")?.getValue();
                            if (spiAlertLevel && (spiAlertLevel.index > 0)) {
                                spiStatus = "12";
                                break;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                printDebugModeLog ("Global View - getSPIStatusForOracle: Failed to parse enable_spi_collections value: " + spiConfigured, null);
            }
        }
    }
    return spiStatus;
}

/**
 * Check if StealthCollect is configured and working
 */
def getSCStatusForOracle(convertOra, pAgent) {
    /*set the default PA status for this case*/
    def paStatus = "0";
    try {
        if (convertOra.get("pa_usability" , specificTimeRange) != null) {
            def paUsability = convertOra.get("pa_usability" , specificTimeRange);
            def paUsabilityList = []
            paUsabilityList.add(paUsability);
            paStatusesList = getInstancePAStatus(convertOra);
            paStatus = getSCStatus(paUsabilityList, paStatusesList, "oracle", isOracleNewVersion, oracleMinimalSCVersion);
        }
    } catch (Exception e) {
        printDebugModeLog ("Global View - getSCStatusForOracle: Missing PA usability topology for " + pAgent.get("name"), null);
    }
    return paStatus;
}


/**
 This function returns the current/average value of the desired OS metric (cpu/disk/memory)
 of the certain 'hostName' parameter.
 If the Host topology doesn't have the required metric, it return's the old metric from the cartridge agent.
 */
def getOSMetricsViaHostNameFromOSCartridge(agent, metricName, dbCartridgeMetric){

    //Metrics path from the Host topology
    //CPU  		= not specified yet
    //MEMORY 	= not specified yet
    //DISK 		= Host/Storage/Disk Utilization
    def hostTopology = null;
    def _name = "***";
    if (agent != null)  {
        _name = agent.name;
        //get the Host topology from the monitoredHost object
        hostTopology = functionHelper.invokeFunction("system:dbwc.convertToHost" , agent);;
    }
    if (hostTopology==null){
        def msg = "Global view has failed to find 'Host' topology for agent: " + _name;
        printDebugModeLog (msg, null);
        return null;
    }

    if (metricName.equals("cpu")){
        if (hostTopology.get("cpus")!=null){
            cpu = hostTopology.get("cpus");
            if (cpu.get("utilization", specificTimeRange)!=null){
                return cpu.get("utilization", specificTimeRange);
            } else {
                return dbCartridgeMetric;
            }
        }
        else
            return dbCartridgeMetric;
    }
    if (metricName.equals("memory")){
        return null;
    }
    if (metricName.equals("disk")){
        if (hostTopology.get("storage")!=null){
            storage = hostTopology.get("storage");
            if (storage.get("diskUtilization", specificTimeRange)!=null){
                return storage.get("diskUtilization", specificTimeRange);
            } else {
                return dbCartridgeMetric;
            }
        }
        else
            return dbCartridgeMetric;
    }

    return null;
}


def getVMWVirtualMachine(agent){
    if (!gIsVFoglight || agent == null ) {
        return null;
    }

    monHost =  functionHelper.invokeFunction("system:dbwc.convertToHost" , agent);
    if (monHost == null ) {
        return null;
    }

    objs = monHost.detail;
    if (objs.size() > 0 ) {
        arr = [];
        for (vHost in objs) {
            if (vHost.topologyType.name.equals("VMWVirtualMachine"))
            //get the latest copy of the topology
                arr.add(topologyService.getObject( vHost.getUniqueId()))
        }

        if (arr.size() == 1){
            return arr.get(0);
        }

        if (arr.size() > 1) {
            //in case that the list contain more then 1 vm topology
            sortedList = arr.sort{ it.lastUpdated};
            return sortedList.get(sortedList.size() - 1);
        }
    }

    return null;
}

def fillVMWareInfo(instanceData, agent) {
    instanceData.set("isVMWareHost", false);
    instanceData.set("vmHostTopologyUUID", "");

    vHost = getVMWVirtualMachine(agent);
    if ( vHost != null ) {
        instanceData.set("isVMWareHost", true);
        instanceData.set("vmHostTopologyUUID" , vHost.get("uuid"));
    }
}

/**
 This function returns the current/average value of the desired OS metric (Cpu/Disk/Memory)
 of the certain inserted vm in the 'vmName' parameter
 */
def getOSMetricsOfCertainVM(agent, metricName){
    //CPU  		= 	<VMWVirtualMachine>/cpus/hostCPUs/utilization
    //MEMORY 	= 	(((<VMWVirtualMachine>/memory/hostMemory/consume/current/average) / (<VMWVirtualMachine>/memory/hostMemory/capacity/current/average))*100)
    //DISK 		= 	N/A

    def vm = getVMWVirtualMachine(agent);

    if (vm==null){
        return null;
    }

    if (metricName.equals("cpu")){ //return the CPU OS metric of the monitored VM
        if (vm.get("cpus")!=null){
            if (vm.get("cpus").get("hostCPUs")!=null){
                if (vm.get("cpus").get("hostCPUs").get("utilization", specificTimeRange)!=null){
                    def cpu = vm.get("cpus").get("hostCPUs").get("utilization", specificTimeRange);
                    return cpu;
                }
            }
        }
    }
    else if (metricName.equals("memory")){//return the Memory OS metric of the monitored VM

        if (vm.memory!=null){

            //get the memory collection
            def memory = vm.get("memory");
            def swapped = (getCurrentAverageForMetric(memory,"swapped")!=null)	?	getCurrentAverageForMetric(memory,"swapped") : 0;
            def shared 	= (getCurrentAverageForMetric(memory,"shared")!=null)	?	getCurrentAverageForMetric(memory,"shared") : 0;
            def balloon = (getCurrentAverageForMetric(memory,"balloon")!=null)	?	getCurrentAverageForMetric(memory,"balloon") : 0;
            def active 	= (getCurrentAverageForMetric(memory,"active")!=null)	?	getCurrentAverageForMetric(memory,"active") : 0;

            def capacity;
            if (memory.get("hostMemory").get("capacity", specificTimeRange).current!=null){
                capacity = memory.get("hostMemory").get("capacity", specificTimeRange).current.average;
            }else {
                return null;
            }


            def totalMetrics = ((double)(((swapped + shared + balloon + active)/1024)/capacity))*100;
            //creating the metric object via function
            def memoryMetric = functionHelper.invokeFunction("system:dbwc_globalview.267","MemoryUtilization", totalMetrics);

            return memoryMetric;
        }
    }
    else if (metricName.equals("disk")){//return the Disk OS metric of the monitored VM
        //this value isn't exist in vFoglight FMS -> N/A
        return null;
    }
    return null;
}

def getCurrentAverageForMetric(collection, metricName){

    if (collection.get(metricName, specificTimeRange)!=null){
        if (collection.get(metricName, specificTimeRange).current!=null){
            if (collection.get(metricName, specificTimeRange).current.average!=null){
                return collection.get(metricName, specificTimeRange).current.average;
            }
        }
    }
    return null;
}


/**
 * Creates a topology for single Oracle instance.
 * Returned data:
 *		instance name, host,version,  alarms, host metrics, up since, workload, PA status
 **/
def createOraInstanceData(clusterAgentContainer, instanceOra, _instanceData, dbType, _isCluster, isAggregate) {

    def instanceUniqueName = "";
    def instanceHostName = "";
    def host = null;
    def instanceName = "";
    def instanceAgent = null;
    def dbVersion = "unknown";
    def topologiesForAlarms = [];
    def hostTopologysForAlarms = [];
    def osMonitoringState = null;
    def isEnableVMCollections = false;
    def oraTopologyObject = instanceOra; // used to set the 'topologyObject' property

    cartrigeVersion = functionHelper.invokeFunction("system:dbwc.activatedCartridgeVersion","DB_Oracle_UI");
    isGreaterCart = functionHelper.invokeFunction("system:dbwc_globalview_spiglobalview.compareVersion",cartrigeVersion,"5.7.1.1");

    if (instanceOra == null) {
        instanceName = clusterAgentContainer.oracleAgent.get("name");
        instanceAgent = clusterAgentContainer.oracleAgent;
        topologiesForAlarms.add(instanceAgent);
        // add instance agents if exist
        for (racInstanceAgent in clusterAgentContainer.racInstanceAgents) {
            topologiesForAlarms.add(racInstanceAgent);
        }
    }
    else if (_isCluster) {
        instanceHostName = "RAC";
        host = instanceOra.get("monitoredHost");
        if(host != null){
            instanceHostName = host.name;
        }
        instanceName = instanceOra.get("name");
        instanceAgent = instanceOra.get("monitoringAgent");
        oraTopologyObject = instanceOra.get("parent_node");

        //adding the relevant topologies for RAC
        topologiesForAlarms.add(instanceOra.get("parent_node"));
        topologiesForAlarms.add(instanceAgent);
        _instanceData.get("instances").each{
            topologiesForAlarms.add(it.get("agent"));

            // remove the agent from the instance agent list in order to not add it twice
            clusterAgentContainer.racInstanceAgents.remove(it.get("agent"));
        }

        // add instance agents that did not succeeded to connect and have no instance topology
        for (racInstanceAgent in clusterAgentContainer.racInstanceAgents) {
            topologiesForAlarms.add(racInstanceAgent);
        }

        osMonitoringState = gOSMonitoringState.get("Ignore");
    }
    else {
        if (isAggregate) {
            // this value is used for the home page link and it holds the RAC agent name followed by the instance agent name
            // instanceUniqueName = instanceOra.get("parent_node").get("name") + ",";
            instanceUniqueName = instanceOra.get("parent_node").get("monitoringAgent").get("name") + ",";
        }
        if(isGreaterCart >= 0) {
            instanceHostName = instanceOra.get("active_host")?.name;
            host = instanceOra.get("active_host");
        }
        else {
            instanceHostName = instanceOra.get("mon_host_name");
            host = instanceOra.get("monitoredHost")
        }
        instanceName = instanceOra.get("mon_instance_name");

        instanceAgent = instanceOra.get("monitoringAgent");

        //adding the relevant topologies for RAC
        topologiesForAlarms.add(instanceOra);
        if ( instanceAgent != null ) {
            topologiesForAlarms.add(instanceAgent);
        }
        topologiesForAlarms.add(instanceOra.get("parent_node").get("database"));

        def isMonnitorOS = false;
        try {
            def aspConfig = instanceAgent.get("asp_config");
            if (aspConfig != null) {
                isMonnitorOS = dataServiceUtils.getStringObservationLatestData(aspConfig, "is_enable_os_collections").equals("true");
                isEnableVMCollections = dataServiceUtils.getStringObservationLatestData(aspConfig, "is_enable_vm_collections").equals("true");
            }
        }catch (e){
            printDebugModeLog("Global View asp_config for agent [" + instanceAgent.get("name") + "] not exist", null);
        }

        // set OS monitoring state
        def isMonnitorIC = false;
        if (isRacOneNodeVar) {
            isMonnitorIC = isRacOneNodeHostMonitoredByIc(instanceOra.get("physical_host_name"));
        }
        else {
            if(isGreaterCart >= 0) {
                isMonnitorIC = isHostMonitoredByIC(instanceOra.get("active_host"));
            }
            else {
                isMonnitorIC = isHostMonitoredByIC(instanceOra.get("monitoredHost"));
            }
        }

        osMonitoringState = getOSMonitoringState(isMonnitorOS, isMonnitorIC);
    }

    try{
        if(isGreaterCart >= 0) {
            hostTopologysForAlarms.add(instanceAgent.get("active_host"));
        }
        else {
            hostTopologysForAlarms.add(instanceAgent.get("monitoredHost"));
        }
    }catch(e){}
    _instanceData.setName(instanceAgent?.get("name"));
    // Identifier
    instanceUniqueName = instanceUniqueName + instanceAgent?.get("name");
    def clusterIdentifier = null;
    if (_instanceData.get("instanceIdentifier") != null) {
        clusterIdentifier = updateClusterIdentifier (instanceUniqueName, _instanceData.instanceIdentifier.cluster);
    } else {
        clusterIdentifier = createClusterIdentifier(instanceUniqueName, dbType, _isCluster);
    }

    def convertOra = null;
    if (instanceOra != null) {
        //Convert - Object to DBO_Instance
        convertOra = functionHelper.invokeFunction("system:oracle.234", instanceOra);
    }

    def _upSince = null;
    //get the Usability Collection from the Agent level or either from the Instance level, depends on the agent version
    def oraUsability = functionHelper.invokeFunction("system:dbwc_globalview_oracle20quick20view.27", instanceAgent, convertOra);
    if (oraUsability != null) {
        dbVersion = oraUsability.getString("db_version");
        def db_start_value;
        try {
            db_start_value = getOracleDBStartTime(oraUsability, cartrigeVersion);
        }  catch (Exception e) {
            //Ignore from this value and continue to parse the agent record
            println("Failed to get Oracle DB start time, reason:" + e.getMessage());
            db_start_value = null;
        }

        if (db_start_value != null){ // 0 ("Unknown"),-1 ("Instance Down"),-2 ("Mounted"), other - Date
            _upSince = new Date(db_start_value);
        }
        else { // if the db_start_value is null
            _upSince = new Date(0); //
        }
    }

    if (instanceOra != null) {
        //PA connection status
        def isSPI = false;
        def paStatus = "0";
        def agentVersion = instanceAgent.get("agentVersion");
        if (UpgradeCheck.isNewerVersion(agentVersion, "5.7.5.0")) {
            isSPI = true;
            paStatus = "10";
        }
        if (_isCluster){ //RAC node
            // use the max PA status from all participating nodes
            for (singleInstanceData in _instanceData.get("instances")) {
                def singleInstancePAState = singleInstanceData.get("paState");
                if (singleInstancePAState != null) {
                    if (singleInstancePAState.compareTo(paStatus) > 0) {
                        paStatus = singleInstancePAState;
                    }
                }
            }
        }
        else { //single oracle instance
            paStatus = getPAUsabilityStatusForOracle(convertOra, instanceAgent, isSPI);
        }

        def cpu = null;
        def memory = null;
        def disk = null;
        def opSystem = convertOra.get("operating_system");

        if (opSystem != null) {
            def osGeneral = opSystem.get("os_general");
            if (osGeneral != null) {
                if (osGeneral.get("DBO_System_CPU_Utilization",specificTimeRange)!=null){
                    cpu = osGeneral.get("DBO_System_CPU_Utilization",specificTimeRange);
                }
                if (osGeneral.get("DB0_Used_RAM_Pct",specificTimeRange)!=null){
                    memory = osGeneral.get("DB0_Used_RAM_Pct",specificTimeRange);
                }
            }
            if (opSystem.get("file_system_io_summary") != null ){
                if (opSystem.get("file_system_io_summary").get("disk_utilization",specificTimeRange)!=null){
                    disk = opSystem.get("file_system_io_summary").get("disk_utilization",specificTimeRange);
                }
            }
        }

        //getting the OS cartridge metric for disk if it's exist
        // ODB-7092
        //disk = getOSMetricsViaHostNameFromOSCartridge(oraTopologyObject, "disk", disk);
        def port = 0;
        fillInstanceData(oraTopologyObject, instanceAgent, dbVersion, _upSince, instanceHostName, instanceName, port, clusterIdentifier, _instanceData, isAggregate, 0, topologiesForAlarms,
                cpu, memory, disk, paStatus, isEnableVMCollections, osMonitoringState, null,hostTopologysForAlarms, host);
        _instanceData.get("instanceIdentifier").set("alias", instanceOra.get("monitoringAgent")?.get("name"));

        try {
            _instanceData.set("Workload", getOracleWorkloadMetric(convertOra, cartrigeVersion));
        } catch (Exception e) {
            printDebugModeLog ("Failed to load Oracle Workload metric" , e);
        }
        waitsValuesOra = [];
        //Workload breakdowns
        if(isSPI) {
            wait_event_data = convertOra.wait_event_data;
            waitsList = ["CPU Usage","User I/O","System I/O","Administrative","Application","Cluster","Commit","Concurrency","Configuration","Network","Scheduler","Queueing","Other"];
            if(wait_event_data != null) {
                for(wait_time in waitsList) {
                    waitsValuesOra.add(wait_event_data?.get("category")?.find{it-> it.name.contains(wait_time)}?.get("wait_time",specificTimeRange));
                }
            }
        }
        _instanceData.set("breakdowns", waitsValuesOra);
    }
    else {
        // instanceOra = null, empty agent shell
        fillInstanceData(instanceAgent, instanceAgent, dbVersion, _upSince, null, instanceName, 0, clusterIdentifier, _instanceData, isAggregate, 0, topologiesForAlarms, null, null, null, "0", false, null, null,hostTopologysForAlarms, null);
        _instanceData.get("instanceIdentifier").set("alias", instanceAgent?.get("name"));
        _instanceData.set("isDataExist" , false);
    }
}

/**
 This function return Oracle Instance workload (active time) metrics
 Return Value:
 from  version 5.7.5.0  return active_time_rate
 Prior version 5.7.5.0  return DBO_Active_Time_Rate

 **/
def getOracleWorkloadMetric(oracleInstanceObj, oracleCartrigeVersion) {
    //from version 5.7.5.0 the workload metric location have been change as we change the category to use Oracle category
    def compareCart = functionHelper.invokeFunction("system:dbwc_globalview_spiglobalview.compareVersion", oracleCartrigeVersion, "5.7.5.0");
    if (compareCart >= 0) {
        return oracleInstanceObj.wait_event_data.active_time_rate;
    } else {
        return oracleInstanceObj.subcategories_wait.DBO_Active_Time_Rate;
    }
}
def checkTheStatusValue(valueStr, oraUsabilityObj){
    def value = valueStr.toLong();
    return value;
}
def checkIfRangeGraterThanTimeInterval (oraUsabilityObj, topologyLastUpdated, timeInterval){
    def value = null;
    def valueStr = dataService.retrieveLatestValue(oraUsabilityObj, "db_start_obs")?.getValue();
    if (valueStr != null)  {
        def endTime = dataService.retrieveLatestValue(oraUsabilityObj, "db_start_obs").("endTime").getTime();
        if(topologyLastUpdated - endTime > timeInterval){
            value = 0; // "Unknown"
        } else {
            value = checkTheStatusValue(valueStr, oraUsabilityObj);
        }
    } else {
        value = 0; // "unknown"
    }
    return value;
}
/**
 Return Oracle Instance start time (Long)
 Return Value (Long):
 from  version 5.7.5.0  return the latest value of the DB start time located "db_start_obs/latest/value"
 Prior version 5.7.5.0  return db_start
 Null - In case the latest value has not find
 **/
def	getOracleDBStartTime(oraUsabilityObj, oracleCartrigeVersion) {
    def value = null;
    def DEF_HOUR_MILISECOND = 60 * 60 * 1000
    def DEF_SIX_MINUTES = 6 * 60 * 1000

    //from version 5.7.5.0 the workload metric location have been change as we change the category to use Oracle category
    def compareCart = functionHelper.invokeFunction("system:dbwc_globalview_spiglobalview.compareVersion", oracleCartrigeVersion, "5.7.5.0");
    if (compareCart >= 0) {
        def topologyLastUpdated = oraUsabilityObj.lastUpdated.getTime(); // get the last updated of topology
        if(System.currentTimeMillis() - topologyLastUpdated > DEF_HOUR_MILISECOND){ // the topology isn't updated
            value = 0 // "Unknown";
        } else {
            value = checkIfRangeGraterThanTimeInterval (oraUsabilityObj, topologyLastUpdated, DEF_SIX_MINUTES);
        }

    } else {
        def startTime = oraUsabilityObj.get("db_start");
        def availabilityMetric = oraUsabilityObj.get("availability");
        def status = functionHelper.invokeFunction("system:dbwc_infrastructure_components_charts20and20gauges.GetStatusForAvailabilityTile", availabilityMetric, DEF_SIX_MINUTES, oraUsabilityObj, startTime);

        if(status.equals("Up")){
            value = startTime;
        } else if(status.equals("Down")){
            value = -1;// is status is "Down" or "Unknown"
        } else {
            value = 0;
        }

    }
    return value;
}


/**
 * To implement ODB-6260 (One Node RAC).
 * The change was isCluster = isCluster && !isRacOneNodeVar.
 * Because One Node RAC needs to behave like an instance in the global view.
 */
def isOracleCluster(aCluster) {
    _isRac = (aCluster != null) && aCluster.get("is_rac");
    isRacOneNodeVar = false; // not declaring 'def' as this variable is used again in the script
    if (_isRac) {
        isRacOneNodeVar = functionHelper.invokeFunction("system:dbwc_globalview.isRacOneNode", aCluster);
    }
    return (_isRac && !isRacOneNodeVar);
}

/*
	This function creates a topology for RAC Oracle instance.
	* Returned data:
		instance name, host,version,  alarms, host metrics, up since, workload, PA status
*/
clusterMap = [:];
def createOraClusterData(oraAgentContainer, clusterData, allHostTopologies) {
    try {
        def _clusterData = clusterData;
        if (_clusterData == null){
            _clusterData = createDataObject("DBWC_GV_GVClusterData");
        }

        def oraDBType = gDBTypeMap.get("Oracle");
        def clusterAgent = oraAgentContainer.oracleAgent.get("cluster");
        def isCluster = isOracleCluster(clusterAgent);

        def clusterIdentifierAgentName;
        if (clusterAgent == null) {
            clusterIdentifierAgentName = oraAgentContainer.oracleAgent.get("name");
        } else {
            clusterIdentifierAgentName = clusterAgent.get("name");
        }
        def _clusterIdentifier = createClusterIdentifier(clusterIdentifierAgentName, oraDBType, isCluster);
        _clusterData.set("clusterIdentifier", _clusterIdentifier);

        if (isCluster) {
            if ( uniqueClusterMap.get(clusterAgent.name) != null ) {
                return null;
            }
            uniqueClusterMap.put(clusterAgent.name , clusterAgent.name);

            def existingClusterInstances = _clusterData.get("instances");
            def newClusterInstances = [];
            def _instanceData;

            for (_inst in clusterAgent.get("instances")) {
                _instanceData = null;

                // search for the existing instance data
                if (existingClusterInstances != null) {
                    for (clusterInstance in existingClusterInstances) {
                        if (_inst.get("monitoringAgent").get("agentID") == clusterInstance.get("agent").get("agentID")) {
                            _instanceData = clusterInstance;
                            break;
                        }
                    }
                }

                if (_instanceData == null) {
                    _instanceData = createDataObject("DBWC_GV_GVInstanceData");
                }

                newClusterInstances.add(_instanceData);

                try {
                    //creation of a RAC Node
                    createOraInstanceData(oraAgentContainer, _inst, _instanceData, oraDBType, false, true);
                }
                catch (Exception e) {
                    printDebugModeLog ("Failed to create RAC instance : "  +_instanceData.get("name") + "\n",e);
                }
            }

            //creation of a RAC Instance
            _clusterData.set("instances", newClusterInstances);
            createOraInstanceData(oraAgentContainer, clusterAgent.get("cluster_aggregated"), _clusterData, oraDBType, true, false);
            updateClusterInstancesRunningState(_clusterData);
        }
        else {
            //creation of a single instance
            def oraInstance = null;
            if ((clusterAgent != null) && (clusterAgent.get("instances").size() > 0)) {
                oraInstance = clusterAgent.get("instances").get(0);
            }
            createOraInstanceData(oraAgentContainer, oraInstance, _clusterData, oraDBType, false, false);
        }

        return _clusterData;
    } catch (Exception e) {
        println("Failed to create Oracle cluster data for agent id [" + String.valueOf(oraAgentContainer?.oracleAgent?.get("agentID")) + "]. error: " + e.getMessage());
        return null;
    }
}

/**
 * Get all MSSQL agent topologies
 **/
def getMSSQLAgents() {
    def mssqlAgentMap = new HashMap();
    if (gActiveCartridgesList.contains("DB_SQL_Server")) {
        def mssqlAgents = #!DBSS_Agent_Model#.getTopologyObjects();
		if (DEBUG_ENABLED) {
			CAT.debug("MSSQL - getMSSQLAgents(): mssqlAgents?.size():${mssqlAgents?.size()}")
		}
                
        for (mssqlAgent in mssqlAgents) {
            if (mssqlAgent == null) {
                continue
            }
            if (!isFederation && gAllAgentMap.get(String.valueOf(mssqlAgent.agentID)) == null) {
                printDebugModeLog ("Agent " + mssqlAgent.name + " not found in the Agent List ("  + mssqlAgent.agentID + ") and will not be shown in the Global view table.", null);
                continue;
            }

            mssqlAgentMap.put(buildDBAgentKey(mssqlAgent), mssqlAgent);
        }
    }
	if (DEBUG_ENABLED) {
		CAT.debug("MSSQL - getMSSQLAgents() exit: mssqlAgentMap?.size():${mssqlAgentMap?.size()}")
	}
    return mssqlAgentMap;
}

/**
 * Get Reporting Services
 **/

def getReportingServices() {
    def ssrsAgentMap = new HashMap();
    try{
        if (gActiveCartridgesList.contains("DB_SQL_Server")) {
            def ssrsServices = #!SS_Reporting_Services#.getTopologyObjects();
            for (ssrsService in ssrsServices) {
                //Check if the agent exists
                def agentId= ssrsService.get("monitoringAgent").get("agentID").toString();
                if (!isFederation && gAllAgentMap.get(agentId) == null) {
                    printDebugModeLog ("Agent " + ssrsService.name + " not found in the Agent List ("  + ssrsService.id + ") and will not be shown in the Global view table.", null);
                    continue;
                }
                ssrsAgentMap.put(extractTopologyKey(ssrsService), ssrsService);
            }
        }
    }catch(e){
        println("Failed to get SSIS Databases with error: "+e.getMessage())
    }
    return ssrsAgentMap;
}

/**
 * Get integretation Services
 **/
def getSsisDatabases() {

    def ssisAgentMap = new HashMap();
    try{
        if (gActiveCartridgesList.contains("DB_SQL_Server")) {
            def ssisServices = #!SS_Integration_Service#.getTopologyObjects();
            for (ssisService in ssisServices) {
                //Check if the agent exists
                def agentId= ssisService.get("monitoringAgent").get("agentID").toString();
                if (!isFederation && gAllAgentMap.get(agentId) == null) {
                    printDebugModeLog ("Agent " + ssisService.name + " not found in the Agent List ("  + ssisService.id + ") and will not be shown in the Global view table.", null);
                    continue;
                }
                //def key = agentId + ":" + ssisService.get("name");
                ssisAgentMap.put(extractTopologyKey(ssisService), ssisService);
            }
        }
    }catch(e){
        println("Failed to get SSIS Databases with error: "+e.getMessage())
    }
    return ssisAgentMap;
}

/**
 * Get all SSAS agent topologies
 **/
def getSSASAgents() {
    def mssqlAgentMap = new HashMap();
    if (gActiveCartridgesList.contains("DB_SQL_Server")) {
        def ssaslAgents = #!SSAS_Agent_Model#.getTopologyObjects();
        for (ssaslAgent in ssaslAgents) {
            if (!isFederation && gAllAgentMap.get(String.valueOf(ssaslAgent.agentID)) == null) {
                println ("Agent " + ssaslAgent.name + " not found in the Agent List ("  + ssaslAgent.agentID + ") and will not be shown in the Global view table.");
                continue;
            }

            mssqlAgentMap.put(buildDBAgentKey(ssaslAgent), ssaslAgent);
        }
    }
    return mssqlAgentMap;
}

/**
 * Get all MySQL agent topologies
 **/
def getMySQLAgents() {
    def mySqlAgentMap = new HashMap();
    if (gActiveCartridgesList.contains("MySQLAgent")) {
        def mysqlAgents = #!MySQLAgent#.getTopologyObjects();
        for (mysqlAgent in mysqlAgents) {
            if (mysqlAgent == null) {
                continue
            }
            mysqlAgent = functionHelper.invokeFunction("system:dbwc.wrapMySQLAgent",mysqlAgent);
            if (!isFederation && gAllAgentMap.get(String.valueOf(mysqlAgent.agentID)) == null) {
                printDebugModeLog ("Agent " + mysqlAgent.name + " not found in the Agent List ("  + mysqlAgent.agentID + ") and will not be shown in the Global view table.", null);
                continue;
            }

            mySqlAgentMap.put(buildDBAgentKey(mysqlAgent), mysqlAgent);
        }
    }
    return mySqlAgentMap;
}

/**
 * Get all MongoDB agent topologies and set MongoDB server topologies mapped by agent id
 **/
def getMongoDBAgentsAndClusters() {
    def mongoDBAgentMap = new HashMap();
    def mongoClusterMap = new HashMap();
    if (gActiveCartridgesList.contains("MongoDBAgent")) {
        def mongoDBAgents = #!MongoDBAgent#.getTopologyObjects();
        def mongoDBClusters = #!Mongo_Cluster#.getTopologyObjects();
        def mongoDBReplSets = #!MongoReplicaSet#.getTopologyObjects().findAll { it.notShardComponent };
        for (def mongoDBAgent in mongoDBAgents) {
            mongoDBAgent = functionHelper.invokeFunction("system:dbwc.wrapMongoAgent",mongoDBAgent);
            if (mongoDBAgent == null) {
                continue
            }
            if (!isFederation && gAllAgentMap.get(String.valueOf(mongoDBAgent.agentID)) == null) {
                printDebugModeLog ("Agent " + mongoDBAgent.name + " not found in the Agent List ("  + mongoDBAgent.agentID + ") and will not be shown in the Global view table.", null);
                continue;
            }
            def agentBuildKey = buildDBAgentKey(mongoDBAgent);
            mongoDBAgentMap.put(agentBuildKey, mongoDBAgent);

            if (mongoClusterMap.get(agentBuildKey) == null) {
                def mongoCluster = mongoDBClusters.find { it.monitoringAgent?.agentID == mongoDBAgent.agentID };
                if (mongoCluster != null) {
                    mongoClusterMap.put(agentBuildKey, mongoCluster);
                } else {
                    def mongoReplSet = mongoDBReplSets.find { it.monitoringAgent?.agentID == mongoDBAgent.agentID };
                    if (mongoReplSet != null) {
                        mongoClusterMap.put(agentBuildKey, mongoReplSet);
                    }
                }
            }
        }
    }
    return [mongoDBAgentMap, mongoClusterMap];
}
/**
 * Get Cassandra agents topologies and set Cassandra clusters topologies mapped by agent id
 **/
def getCassandraAgents() {
    def cassandraAgentMap = new HashMap();
    def cassandraClusterMap = new HashMap();
    if (gActiveCartridgesList.contains("CassandraAgent")) {
        def cassandraAgents = #!CassandraAgent#.getTopologyObjects();
        def  cassandraClusters = #!CassandraCluster where monitoringAgent != $null#.getTopologyObjects();
        def cassandraAgent;
        for(cassandraCluster in cassandraClusters) {
            cassandraAgent = cassandraCluster?.monitoringAgent;
            if (cassandraCluster == null || cassandraAgent == null) {
                continue
            }
            if (!isFederation && gAllAgentMap.get(String.valueOf(cassandraAgent.agentID)) == null) {
                printDebugModeLog ("Agent " + cassandraAgent.name + " not found in the Agent List ("  + cassandraAgent.agentID + ") and will not be shown in the Global view table.", null);
                continue;
            }
            cassandraAgentMap.put(buildDBAgentKey(cassandraAgent), cassandraAgent);
            //Set cluster in cassandraClusterMap
            clusterInMap = cassandraClusterMap.get(buildDBAgentKey(cassandraAgent));
            if(clusterInMap == null) {
                cassandraClusterMap.put(buildDBAgentKey(cassandraAgent) , cassandraCluster);
            }
            else { //Get the updated cluster if the cluster for the same agent already exists
                def activeCluster = (clusterInMap.get("lastUpdated") > cassandraCluster.get("lastUpdated")) ? clusterInMap : cassandraCluster;
                cassandraClusterMap.put(buildDBAgentKey(cassandraAgent) , activeCluster);
            }
        }
    }
    return [cassandraAgentMap, cassandraClusterMap];
}
/**
 * Get all PostgreSQL agent topologies
 **/
def getPostgreSQLAgents() {
    def postgreSQLAgentMap = new HashMap();
    if (gActiveCartridgesList.contains("PostgreSQLAgent")) {
        def postgresqlAgents = #!PostgreSQLAgent#.getTopologyObjects();
        for (postgresqlAgent in postgresqlAgents) {
            if (postgresqlAgent == null) {
                continue
            }
            if (!isFederation && gAllAgentMap.get(String.valueOf(postgresqlAgent.agentID)) == null) {
                printDebugModeLog ("Agent " + postgresqlAgent.name + " not found in the Agent List ("  + postgresqlAgent.agentID + ") and will not be shown in the Global view table.", null);
                continue;
            }

            postgreSQLAgentMap.put(buildDBAgentKey(postgresqlAgent), postgresqlAgent);
        }
    }
    return postgreSQLAgentMap;
}

/**
 * Get Oracle agent topologies
 **/
def getOracleAgents() {
    def oracleAgentMap = new HashMap(); // agent unique key -> OracleAgentContainer.
    def racAgentMap = new HashMap();    // rac key -> OracleAgentContainer. used for RAC -> Instances agent link.
    def racInstancesAgents = [];

    if (gActiveCartridgesList.contains("DB_Oracle")) {
        def oracleAgents = #!DBO_Agent_Model#.getTopologyObjects();
		if (DEBUG_ENABLED) {
			CAT.debug("ORACLE - getOracleAgents(): oracleAgents?.size():${oracleAgents?.size()}")
		}
        for (oracleAgent in oracleAgents) {
            if (oracleAgent == null) {
                continue
            }
            if (!isFederation && gAllAgentMap.get(String.valueOf(oracleAgent.agentID)) == null) {
                printDebugModeLog ("Agent " + oracleAgent.name + " not found in the Agent List ("  + oracleAgent.agentID + ") and will not be shown in the Global view table.", null);
                continue;
            }

            if (!oracleAgent.get("type").equals("DB_Oracle_RAC_Instance")) {
                def oracleAgentContainer = new OracleAgentContainer();
                oracleAgentContainer.oracleAgent = oracleAgent;
                oracleAgentMap.put(buildDBAgentKey(oracleAgent), oracleAgentContainer);
                racAgentMap.put(buildDBRacAgentKey(oracleAgent), oracleAgentContainer);
            } else {
                racInstancesAgents.add(oracleAgent);
            }
        }

        // scan the RAC instance agents and add them to the relevant rac container
        try {
            for (racInstanceAgent in racInstancesAgents) {
                if (racInstanceAgent.get("asp_config") != null) {
                    def clusterAgentName = dataService.retrieveLatestValue(racInstanceAgent.get("asp_config"), "cluster_agent_name")?.getValue();
                    if (clusterAgentName != null) {
                        def racKey = racInstanceAgent.get("hostName") + ":" + clusterAgentName;
                        def oracleAgentContainer = racAgentMap.get(racKey);
                        if (oracleAgentContainer != null) {
                            oracleAgentContainer.racInstanceAgents.add(racInstanceAgent);
                        }
                    }
                }
            }
        } catch (Exception e) {
            printDebugModeLog("Failed to retrieve cluster_agent_name value.", e);
            // the try - catch block is on the entire loop as if the observation is missing, it will be missed for the entire agents
        }
    }

	if (DEBUG_ENABLED) {
		CAT.debug("ORACLE - getOracleAgents() exit: oracleAgentMap?.size():${oracleAgentMap?.size()}")
	}
    return oracleAgentMap;
}

/**
 * Build Oracle and MSSQL agent key
 **/
def buildDBAgentKey(dbAgents) {
    if (dbAgents == null) {
        return null
    }
    return dbAgents.agentID + ":" + dbAgents.get("name");
}
/**
 * Build Oracle Rac agent key
 **/
def buildDBRacAgentKey(dbAgents) {
    if (dbAgents == null) {
        return null
    }
    return dbAgents.get("hostName") + ":" + dbAgents.get("name");
}

/**
 * This function is responsible for getting the actual DB2Monitor agent for the specifiec instance parameter
 */
def getDB2MonitoredAgent(aHost, aInstance, aAllDB2MonitorAgents){
    //the function result
    _getDB2MonitoredAgent = null;
    if ((aHost != null) && (aInstance != null) && (aAllDB2MonitorAgents != null)){
        instanceName =  aInstance.getName().trim();
        //getting through all over the DB2 Monitor Agents
        for (db2Mon in aAllDB2MonitorAgents){
            //if ((!db2Mon.runningStateObservation.latest.value.name.trim().equals("Unknown")) && (db2Mon.host.name.trim().equals(aHost.name.trim()))){
            if (db2Mon.host.name.trim().equals(aHost.name.trim())){
                for (table in db2Mon.tables){
                    //if the table is DB2Instances and if he has the instance name
                    if ((table.name.equals("DB2Instances")) && (table.Instance.equals(instanceName))){
                        _getDB2MonitoredAgent = db2Mon;
                        //return the actual DB2Monitor
                        return _getDB2MonitoredAgent;
                    }
                }
            }
        }

    }
    if (_getDB2MonitoredAgent==null){
        printDebugModeLog ("DB2MonitoredAgent topology couldn't be found for " + aInstance.name + " instance, on host: " + aHost.name, null);
    }
    return _getDB2MonitoredAgent;
}

/*
	This function creates a topology for a DB2 instance.
	* Returned data:
		instance name, host, alarms, host metrics
	* Not returned data:
		version, up since, workload, PA status
*/
def createDB2LegacyClusterData(instance, clusterData, allDB2MonitorAgents){

    if( clusterData == null ){
        clusterData = createDataObject( "DBWC_GV_GVClusterData");
    }
    clusterData.set( "name",           instance.get("name"));
    clusterData.set( "topologyObject", instance )

    def pAgent       = instance.get("db2agent");
    def agentName    = pAgent.get("agentInstance");
    def DB2DBtype    = gDBTypeMap.get("DB2");

    def clusterIdentifier = null

    if(clusterData.instanceIdentifier != null){
        clusterIdentifier = updateClusterIdentifier(agentName, clusterData.instanceIdentifier.cluster)
    }
    else{
        clusterIdentifier = createClusterIdentifier(agentName, DB2DBtype, false)
    }

    //getting the DB2Monitor agent, if it's !=null we add it to the topologies list
    DB2MonitorAgent = getDB2MonitoredAgent(pAgent.monitoredHost, instance, allDB2MonitorAgents)

    clusterData.set("clusterIdentifier", clusterIdentifier);
    clusterData.setName(/*pAgent.get("name")*/ agentName.toUpperCase());

    def db2instanceTable = null

    try{
        db2instanceTable = server.QueryService.queryTopologyObjects("DB2Monitor_DB2Instances: Instance = '" + instance.get("name") + "'", null).toArray()[0]
    }
    catch(e){
    }

    def upSince      = null
    def dbVersion    = ""; //no version field is supplied
    def hostName     = instance.get("host").getName();
    def host         = instance.get("host");
    def instanceName = agentName
    def paStatus     = "-1"
    def cpu          = null
    def memory       = null
    def disk         = null
    def workload     = null

    try{
        def clusterTpl = clusterData.get("topologyObject");
        //getting a DB2Instance object
        def tpl = functionHelper.invokeFunction("system:dbwc_globalview_db2_quick_view.19",clusterTpl);

        //getting the DB2Version from the DB2Monitor Agent
        for (table in DB2MonitorAgent.tables){
            if (table.get("topologyTypeName").equals("DB2Monitor_DB2Instances")){
                dbVersion = table.DB2Version;
            }
        }
        /* if the value equals "Unknown" so we don't show it, instead of it we show ""   */
        dbVersion = (dbVersion.equals("Unknown")?"":dbVersion);

        def host1 = tpl.get("host");
        def host2 = host1.get("host");
        def cpus = host2.get("cpus");
        def mem = host2.get("memory");
        def storage = host2.get("storage");

        if (cpus!=null)
            cpu = cpus.get("utilization");

        if (mem!=null)
            memory = mem.get("utilization");

        if (storage!=null)
            disk = storage.get("diskUtilization");

        //getting the wordload for the DB2 cluster by using a function
        def tables = [];
        //call to: get DB2Agent Tables function that returns the tables property from the pAgent
        tables = functionHelper.invokeFunction("system:dbwc_globalview_db2_quick_view.38",pAgent);

        def clientConn = null;
        for (tbl in tables){
            if (tbl.get("name").equals("Client_Connection")){
                clientConn = tbl;
            }
        }
        if (clientConn!=null){
            workload = clientConn.get("client_cpu", specificTimeRange);
        }

        clusterData.set("Workload", workload);
    }
    catch(Exception e){
        println "DB2 Exception:" + e.getMessage();
    }

    def topologiesForAlarms = []

    //topologiesForAlarms.add(instance);//DB2Instance
    topologiesForAlarms.add(pAgent.monitoringAgent);//DB2Agent
    topologiesForAlarms.add(pAgent.monitoredHost);//Host
    if (DB2MonitorAgent!=null){
        topologiesForAlarms.add(DB2MonitorAgent)//DB2MonitorAgent topology
    }
    /* adding the databases topologies */
    def databases = instance.get("databases", specificTimeRange);
    for (db in databases){
        topologiesForAlarms.add(db);
    }

    def isEnableVMCollections = false;

    // set OS monitoring state
    def osMonitoringState = gOSMonitoringState.get("Ignore");

    fillInstanceData(instance, pAgent, dbVersion, upSince, hostName, instanceName, 0, clusterIdentifier, clusterData, false, 0, topologiesForAlarms,
            cpu, memory, disk, paStatus, isEnableVMCollections, osMonitoringState, null,null, host);
    clusterData.get("instanceIdentifier").set("alias", agentName);

    return clusterData;
}


/*
	This function creates a topology for Sybase MDA instance.
	* Returned data:
		instance name, host,version, up since, workload, alarms, host metrics
	* Not returned data:
		 PA status
*/

def createSybaseMDAClusterData(agent, clusterData){
    if( clusterData == null ){
        clusterData = createDataObject( "DBWC_GV_GVClusterData");
    }
    //println "agent: $agent, clusterData: $clusterData"

    clusterData.set( "name",           agent.get("name"));
    clusterData.set( "topologyObject", agent )

    def pAgent       = agent.get("monitoringAgent");
    def agentName    = agent.get("agentInstance");
    def SybaseDBtype = gDBTypeMap.get("SYBASE");

    def clusterIdentifier = null

    if(clusterData.instanceIdentifier != null){
        clusterIdentifier = updateClusterIdentifier(agentName, clusterData.instanceIdentifier.cluster)
    }
    else{
        clusterIdentifier = createClusterIdentifier(agentName, SybaseDBtype, false)
    }

    clusterData.set("clusterIdentifier", clusterIdentifier);
    clusterData.setName(pAgent.get("name"));

    def upSince = functionHelper.invokeFunction("system:dbwc_globalview_sybase20mda20quick20view.35", pAgent, "dummy");
    if (upSince.equals("Indeterminate or Down")){
        upSince      = null;
    } else if (upSince != null) {
        upSince = Date.parse("MM/dd/yy HH:mm", upSince);
    }

    def dbVersion    = functionHelper.invokeFunction( "system:dbwc_globalview_sybase20mda20quick20view.22", pAgent, "short" );
    def serverInfo = agent.tables.find{ it.name.equals("SybaseServerInfo") && it.server_type.latest.value.equals("ASE") } ;

    def hostName     = "";
    if (serverInfo!=null){
        try {
            hostName     = dataServiceUtils.getStringObservationLatestData(serverInfo, "host");
        } catch (Exception e){
            hostName     = "";
        }
    } else {
        agent.get("monitoredHost")?.get("name") ;
    }

    def instanceName = agentName
    def paStatus     = "-1"
    def cpu          = null
    def memory       = null
    def disk         = null
    def workload     = null
    def isEnableVMCollections = false;

    try{
        def real_agent_name = agent.get("name");
        def container = agent.get("tables").find{ it.get("name") == "SystemWaitEvents" }
        if (container.get("mda_wait_class_total_wait_delta")!=null){
            workload = container.get("mda_wait_class_total_wait_delta");
        }
        clusterData.set("Workload", workload);
    }
    catch(Exception e){
        def msg = "Could not get Workload metric for Sybase MDA agent with name: " + agentName;
        printDebugModeLog (msg, null);
    }

    //get the hostTopology to get the OS metrics from it.
    def hostTopologies = functionHelper.invokeFunction( "system:dbwc_globalview_sybase20mda20quick20view.36", hostName);
    def hostTopology = null;
    try{
        if ((hostTopologies != null) && (hostTopologies.size() > 0)) {
            hostTopology = hostTopologies.get(0);
            if (hostTopology != null) {
                cpu    = hostTopology.get("cpus")?.get("utilization");
                memory = hostTopology.get("memory")?.get("utilization");
                disk   = hostTopology.get("storage")?.get("diskUtilization");


                //used for implementing the metric bar in the tree table
                if (cpu != null) {
                    cpu.set("name", "SybaseCPU");
                }
                if (memory != null) {
                    memory.set("name", "SybaseMemory");
                }
                if (disk != null) {
                    disk.set("name", "SybaseDisk");
                }
            }
        }
    } catch (Exception e) {
        println("Sybase MDA hostTopology error: " + e.getMessage());
    }

    // set OS monitoring state
    def isMonnitorIC = isHostMonitoredByIC(hostTopology);
    def osMonitoringState = getOSMonitoringState(false, isMonnitorIC);

    def topologiesForAlarms = []

    topologiesForAlarms.add( agent )
    if (hostTopology != null){
        topologiesForAlarms.add(hostTopology);
    }
    def hostTopologysForAlarms = [];
    try{
        hostTopologysForAlarms.add(hostTopology);
    }catch(e){}

    fillInstanceData(agent, pAgent, dbVersion, upSince, hostName, instanceName, 0, clusterIdentifier, clusterData, false, 0, topologiesForAlarms,
            cpu, memory, disk, paStatus, isEnableVMCollections, osMonitoringState, null,hostTopologysForAlarms, hostTopology);

    clusterData.get("instanceIdentifier").set("alias", agentName);

    return clusterData;
}


/*
	This function creates a topology for Sybase RS instance.
	* Returned data:
		instance name, host,version,  alarms, host metrics
	* Not returned data:
		 up since, workload, PA status
*/
def createSybaseRSClusterData(agent, clusterData){

    if( clusterData == null ){
        clusterData = createDataObject( "DBWC_GV_GVClusterData" )
    }

    clusterData.set( "name",           agent.get("name") )
    clusterData.set( "topologyObject", agent )

    def pAgent       = agent.get("monitoringAgent");
    def agentName    = agent.get("agentInstance");
    def SybaseDBtype = gDBTypeMap.get("SYBASE");


    def clusterIdentifier = null

    if(clusterData.instanceIdentifier != null){
        clusterIdentifier = updateClusterIdentifier(agentName, clusterData.instanceIdentifier.cluster)
    }
    else{
        clusterIdentifier = createClusterIdentifier(agentName, SybaseDBtype, false)
    }

    clusterData.set("clusterIdentifier", clusterIdentifier)
    clusterData.setName(pAgent.get("name"))

    def upSince      = null;
    def dbVersion    = functionHelper.invokeFunction( "system:dbwc_globalview_sybase20rs20quick20view.32", pAgent, "short" )

    //get the collection for the host name
    def serverInfo 	 = null;
    for (table in pAgent.get("tables")){
        if (table.get("topologyTypeName").equals("Sybase_RS_RS_Info")){
            serverInfo = table;
            break;
        }
    }
    //retireiving the host name property
    def hostName     = "";
    if (serverInfo!=null){
        try {
            hostName     = dataServiceUtils.getStringObservationLatestData(serverInfo, "rs_host");
        }
        catch (Exception e){
            hostName = "";
        }
    }

    def instanceName = agentName
    def paStatus     = "-1"
    def isEnableVMCollections = false;
    def cpu          = null
    def memory       = null
    def disk         = null

    //get the hostTopology to get the OS metrics from it.
    def hostTopologies = functionHelper.invokeFunction( "system:dbwc_globalview_sybase20mda20quick20view.36", hostName);

    try {
        if ((hostTopologies != null) && (hostTopologies.size() > 0)) {
            def hostTopology = hostTopologies.get(0);
            if (hostTopology != null) {
                cpu    = hostTopology.get("cpus")?.get("utilization");
                memory = hostTopology.get("memory")?.get("utilization");
                disk   = hostTopology.get("storage")?.get("diskUtilization");
            }
        }
    } catch (Exception e) {
        println("Sybase RS hostTopology error: " + e.getMessage());
    }

    // set OS monitoring state
    def isMonnitorIC = isHostMonitoredByIC(pAgent.get("monitoredHost"));
    def osMonitoringState = getOSMonitoringState(false, isMonnitorIC);

    def topologiesForAlarms = []

    topologiesForAlarms.add( agent )
    if (pAgent.get("monitoredHost")!=null){
        topologiesForAlarms.add(pAgent.get("monitoredHost"));
    }

    def hostTopologysForAlarms = [];
    try{
        hostTopologysForAlarms.add(pAgent.get("monitoredHost"));
    }catch(e){}

    fillInstanceData(agent, pAgent, dbVersion, upSince, hostName, instanceName, 0, clusterIdentifier, clusterData, false, 0, topologiesForAlarms,
            cpu, memory, disk, paStatus, isEnableVMCollections, osMonitoringState, null,hostTopologysForAlarms, null);

    clusterData.get("instanceIdentifier").set("alias", agentName);

    return clusterData;
}

/**
 * Build a map of DBWC_GV_OSMonitoringState
 **/
def getOSMonitoringStateMap() {
    def osMonitoringStateMap = [:];
    queryService.queryTopologyObjects("DBWC_GV_OSMonitoringState").each { osMonitoringStateMap.put(it.get("value"), it) };
    return osMonitoringStateMap;
}

def getOSMonitoringState(isMonitoredByDB, isMonitoredByIC) {
    def osMonitoringState;
    if (isMonitoredByDB) {
        if (isMonitoredByIC) {
            osMonitoringState = gOSMonitoringState.get("Full");
        } else {
            osMonitoringState = gOSMonitoringState.get("DB");
        }
    } else {
        if (isMonitoredByIC) {
            osMonitoringState = gOSMonitoringState.get("IC");
        } else {
            osMonitoringState = gOSMonitoringState.get("None");
        }
    }
    return osMonitoringState;
}

def isHostMonitoredByIC(host) {
    return host?.get("monitoringAgent") != null;
}

def isRacOneNodeHostMonitoredByIc(physicalHostName) {
    def query = "!Agent where topologyTypeName='ICAgent' and monitoredHost.name='" + physicalHostName + "'";
    def icAgent = queryService.queryTopologyObjects(query);
    return (icAgent != null && icAgent.size() > 0);
}

def getSybaseMDAAgents(){
    def map = [:]

    if (gActiveCartridgesList.contains("Sybase_MDA") || gActiveCartridgesList.contains("DB_Sybase")) {
        try{
            functionHelper.invokeQuery( "system:sybasemda.55" ).each{
                if (isFederation || gAllAgentMap.get(String.valueOf(it.agentID)) != null) {
                    map.put( it.get("name").toUpperCase(), it );
                } else {
                    printDebugModeLog ("Agent " + it.get("name") + " not found in the Agent List ("  + it.agentID + ") and will not be shown in the Global view table.", null);
                }
            }
        }
        catch(Exception e){
            printDebugModeLog ("Error has occurred while trying to execute function: getSybaseMDAAgents()\n", e);
        }
    }

    return map;
}

def getSybaseRSAgents(){
    def map = [:]

    if (gActiveCartridgesList.contains("Sybase_RS") || gActiveCartridgesList.contains("DB_Sybase")) {
        try{
            functionHelper.invokeQuery( "system:sybasers.41" ).each{
                if (isFederation || gAllAgentMap.get(String.valueOf(it.agentID)) != null) {
                    map.put( it.get("name").toUpperCase(), it );
                } else {
                    printDebugModeLog ("Agent " + it.get("name") + " not found in the Agent List ("  + it.agentID + ") and will not be shown in the Global view table.", null);
                }
            }
        }
        catch(Exception e){
            printDebugModeLog ("Error has occurred while trying to execute function: getSybaseRSAgents()\n", e);
        }
    }

    return map;
}


def getDB2LegacyInstances(){
    def map = [:]

    if (gActiveCartridgesList.contains("DB2")) {
        try{
            server.QueryService.queryTopologyObjects("DB2Instance", null).each{
                if (it!=null){
                    if (it.get("db2agent")!=null){
                        if (it.get("db2agent").get("agentInstance")!=null){
                            map.put( it.get("db2agent").get("agentInstance").toUpperCase(), it);
                        }
                    }
                }
            }
        }
        catch(Exception e){
            printDebugModeLog ("Error has occurred while trying to execute function: getDB2LegacyInstances()\n", e);
        }
    }

    return map;
}

def getAllDB2LegacyMonitorAgents(){
    def list = []

    if (gActiveCartridgesList.contains("DB2")) {
        try{
            functionHelper.invokeQuery("system:dbwc_globalview_db2_quick_view.30").each{
                list.add(it);
            }
        }
        catch(Exception e){
            printDebugModeLog ("Error has occurred while trying to execute function: getAllDB2LegacyMonitorAgents()\n", e);
        }
    }

    return list;
}

def getSQLAzureInstances(){
    def map = [:];

    if (gActiveCartridgesList.contains("DB_Azure")) {
        try{
            functionHelper.invokeQuery( "system:dbwc_globalview.256" ).each{
                map.put( it.get("name").toUpperCase(), it );
            }
        }
        catch(Exception e){
            printDebugModeLog ("Error has occurred while trying to execute function: getSQLAzureInstances()\n", e);
        }
    }

    return map;
}

/*
This function returns all the DB2_Instances topologies in a map
*/
def getAllDB2AraratInstances(){
    def map = [:];
    if (gActiveCartridgesList.contains("DB_DB2")) {
        try{
            functionHelper.invokeQuery( "system:dbwc_globalview_quick_views_db2_instancequickview.1" ).each{
                map.put( it.get("name").toUpperCase(), it );
            }
        }
        catch(Exception e){
            printDebugModeLog ("Error has occurred while trying to execute function: getAllDB2AraratInstances()\n", e);
        }
    }
    return map;
}

/**
 * Get all DB2 agent topologies
 **/
def getDB2Agents() {
    def db2AgentMap = new HashMap();
    if (gActiveCartridgesList.contains("DB_DB2")) {
        def db2Agents = #!DB2_Agent_Model#.getTopologyObjects();
		if (DEBUG_ENABLED) {
			CAT.debug("DB2 - getDB2Agents(): db2Agents?.size():${db2Agents?.size()}")
		}
        for (db2Agent in db2Agents) {
            if (!isFederation && gAllAgentMap.get(String.valueOf(db2Agent.agentID)) == null) {
                printDebugModeLog ("Agent " + db2Agent.name + " not found in the Agent List ("  + db2Agent.agentID + ") and will not be shown in the Global view table.", null);
                continue;
            }

            db2AgentMap.put(buildDBAgentKey(db2Agent), db2Agent);
        }
    }
	if (DEBUG_ENABLED) {
		CAT.debug("DB2 - getDB2Agents() exit: db2AgentMap?.size():${db2AgentMap?.size()}")
	}
    return db2AgentMap;
}

/*
	This function returns all the Host topologies in a map. Those topologies are created by the OS Cartridge
*/
def getAllHostTopologies(){
    def map = [:];
    try{
        functionHelper.invokeQuery( "system:dbwc_globalview.305" ).each{
            map.put( it.get("name").toLowerCase(), it );
        }
    }
    catch(Exception e){
        printDebugModeLog ("Error has occurred while trying to execute function: getAllHostTopologies()\n", e);
    }

    return map;
}


realAgentMap = [:] //  [agentID => agent] , this map is used for upgrade check that requires ag

def getAllAgentMap() {
    try {
        agentService = server.AgentService;
        agentList = agentService.findAll();
        //def agentMap = new HashMap(); // IDO: this seems to be irrelevant
        for (_agent in agentList) {
            //agentMap.put(_agent.getId() , _agent.getName());
            realAgentMap.put( _agent.getId(), _agent );
        }

        //return agentMap;
        return realAgentMap;
    }
    catch(Exception e){
        CAT.error("Error has occurred while trying to execute function: getAllAgentMap()\n", e);
    }
}

/**
 * Build a map of DBWC_GV_DatabaseTypeEnum
 **/
def getAllDBTypeMap() {
    def dbTypeMap = [:];
    queryService.queryTopologyObjects("DBWC_GV_DatabaseTypeEnum").each { dbTypeMap.put(it.get("value"), it) };
    return dbTypeMap;
}

/**
 * Build a map of DBWC_GV_Severity
 **/
def getSeverityTypeMap() {
    def severityTypeMap = [:];
    queryService.queryTopologyObjects("DBWC_GV_Severity").each { severityTypeMap.put(Integer.valueOf(it.get("value")), it) };
    return severityTypeMap;
}


/**
 * support PostgreSQL & MySQL integration from x version
 **/
def checkCartridgeVersion(cartridgeName, cartrigeVersion) {
    def result = true;
    if (cartridgeName.equals("PostgreSQLAgent") || cartridgeName.equals("MySQLAgent") || cartridgeName.equals("MongoDBAgent") || cartridgeName.equals("CassandraAgent")) {
        try {
            def registryName;
            switch (cartridgeName) {
                case "PostgreSQLAgent":
                    registryName = "GV_PostgreSQL_Min_Version";
                    break;
                case "MySQLAgent":
                    registryName = "GV_MySQL_Min_Version";
                    break;
                case "MongoDBAgent":
                    registryName = "GV_MongoDB_Min_Version";
                    break;
                case "CassandraAgent":
                    registryName = "GV_Cassandra_Min_Version";
                    break;
            }
            def registry = registryService.getRegistryVariable(registryName);
            def registryValue = registry.getGlobalDefault().getDefaultValue();
            compare = functionHelper.invokeFunction("system:dbwc_globalview_spiglobalview.compareVersion", cartrigeVersion, registryValue);
            if (compare < 0) {
                result = false;
            }
        } catch (Exception e) {
            println("An Exception occurred in checkCartridgeVersion error: " + e.getMessage());
            result = false;
        }
    }
    return result;
}

def getActiveCartridgesList() {
    def cartridgeList = [];
    def cartridgeService = server.get("CartridgeService");
    def cartridges = cartridgeService.listCartridges();
    for (cartridge in cartridges) {
        if (cartridge.getCartridgeStatus() == cartridge.getCartridgeStatus().ACTIVATED && checkCartridgeVersion(cartridge.getName(), cartridge.getVersion())) {
            cartridgeList.add(cartridge.getIdentity().getName());
        }
    }
    return cartridgeList;
}

/**
 * Extract the topology key from the topology saved in the GV cluster's object
 **/
def extractTopologyKey(topologyObject) {
    def topologyKey = null;
    if (topologyObject != null) {
        def topologyTypeName = topologyObject.get("topologyTypeName");
        if (topologyTypeName.contains("DB2Instance")) {
            topologyKey = topologyObject.get("db2agent").get("agentInstance").toUpperCase();
        } else if (topologyTypeName.equals("DBSS_Clusters_Data") || topologyTypeName.equals("DBSS_Instance") ||
                topologyTypeName.equals("DBO_Instance_Clusters_Data") || topologyTypeName.equals("DBO_Rac_Clusters_Data") ||
                topologyTypeName.equals("DB2_Instance") || topologyTypeName.equals("DB2_Database") ||
                topologyTypeName.equals("Mongo_Cluster") || topologyTypeName.equals("MongoReplicaSet")) {
            topologyKey = buildDBAgentKey(topologyObject.get("monitoringAgent"));
        }
        else if (topologyTypeName.equals("DBSS_Reporting_Services_General") || topologyTypeName.equals("DBSS_Database")) {
            topologyKey = (topologyObject.get("name") + topologyObject.get("monitoringAgent")?.get("name")).toUpperCase();
        }else if (topologyTypeName.equals("MySQLAgent") || topologyTypeName.equals("PostgreSQLAgent") || topologyTypeName.equals("MongoServer") || topologyTypeName.equals("CassandraCluster")) {
            def monitoringAgent = topologyObject.get("monitoringAgent");
            topologyKey = monitoringAgent ? buildDBAgentKey(monitoringAgent) : null;
        } else if (topologyTypeName.equals("DBO_Instance")) {
            // support one rac node where we should extract the RAC agent from the cluster topology and not use the instance agent.
            // this change should no affect regular instances as both cluster's and instance's agents are the same
            topologyKey = buildDBAgentKey(topologyObject.get("parent_node").get("monitoringAgent"));
        } else if (topologyTypeName.equals("DBSS_Agent_Model") || topologyTypeName.equals("DBO_Agent_Model") || topologyTypeName.equals("DB2_Agent_Model")|| topologyTypeName.equals("SSAS_Agent_Model")) {
            topologyKey = buildDBAgentKey(topologyObject);
        }else if(topologyTypeName.equals("SS_Integration_Service") || topologyTypeName.equals("SS_Reporting_Services")){
            topologyKey = (topologyObject.get("name") + topologyObject.get("monitoringAgent")?.get("name")).toUpperCase();
        }
        else {
            // specifying the relevant name for all other instances
            topologyKey = topologyObject.getName().toUpperCase();
        }
    }
    return topologyKey;
}

/**
 * Call to a persistence function to enable the usage feedback (call home) collection.
 * This call should be done only once, so it check a registry variable value accordingly
 */
def activateUsageFeedbackCollection() {
    try {
        def isEnableUsageFeedbackRegistry = registryService.getRegistryVariable("GV_DB_Enable_Usage_Feedback");
        if (isEnableUsageFeedbackRegistry.getGlobalDefault().getDefaultValue() && !isFederation) {
            // call to an external persistence function to enable the usage feedback collection
            def enabled = enableUsageFeedbackCollection();
            println("Enable Usage Feedback Collection by DB cartridge. Result: " + String.valueOf(enabled));

            // change the registry value to false
            isEnableUsageFeedbackRegistry.getGlobalDefault().setDefaultValue(false);
            registryService.saveRegistryVariable(isEnableUsageFeedbackRegistry);
        }
    } catch (Exception e) {
        println("An Exception occurred while enable Usage Feedback collection by DB cartridge. error: " + e.getMessage());
    }
}

/**
 * This function prints to the log on DEBUG mode
 **/
def printDebugModeLog(logMessage, e) {
    if (!DEBUG_ENABLED) {
        return
    }
    if (e != null) {
        CAT.debug("${logMessage}, Exception happpens: e:${e}, error message:${e.getMessage()}.")
    } else {
        CAT.debug(logMessage)
    }
}

def topologyBasicToString(topology) {
    if (topology == null) {
        return "null"
    }
    return "(topologyTypeName:${topology?.topologyTypeName}, name:${topology?.name}, uniqueId:${topology?.uniqueId}, lastUpdated:${topology?.lastUpdated})"
}

def DBWC_GV_GVClusterData_ToString(gvClusterData) {//Type is "DBWC_GV_GVClusterData"
    if (gvClusterData == null) {
        return "DBWC_GV_GVClusterData(null)"
    }
    def sb = new StringBuilder()
    sb << "DBWC_GV_GVClusterData (\n"
    sb << "  name: ${gvClusterData?.get('name')}\n"
    sb << "  uniqueId: ${gvClusterData?.get('uniqueId')}\n"
    sb << ")\n"
    return sb.toString()
}

def DBWC_GV_GVInstanceData_ToString(gvInstanceData) {//Type is "DBWC_GV_GVInstanceData"
    if (gvInstanceData == null) {
        return "DBWC_GV_GVInstanceData(null)"
    }
    def sb = new StringBuilder()
    sb << "DBWC_GV_GVInstanceData (\n"
    sb << "  name: ${gvInstanceData?.get('name')}\n"
    sb << "  uniqueId: ${gvInstanceData?.get('uniqueId')}\n"
    sb << "  topologyObject: ${topologyBasicToString(gvInstanceData?.get('topologyObject'))}\n"
    sb << "  agent: ${Agent_ToString(gvInstanceData?.get('agent'))}\n"
    sb << "  databaseUpSince: ${gvInstanceData?.get('databaseUpSince')}\n"
    sb << ")\n"
    return sb.toString()
}


def Agent_ToString(myAgent) {//Type is "Agent", such as "DB2_Agent_Model"
    if (myAgent == null) {
        return "Agent(null)"
    }
    return "Agent(agentID:${myAgent?.agentID}, ${topologyBasicToString(myAgent)})"
}

def DB2_Database_ToString(db2Database) {
    if (db2Database == null) {
        return "DB2_Database(null)"
    }
    return "DB2_Database(name:${db2Database?.name}, db_name:${db2Database?.db_name}, uniqueId:${db2Database?.uniqueId})"
}

def DB2_Instance_toString(db2Instance) {
    if (db2Instance == null) {
        return "DB2_Instance( null )"
    }
    def sb = new StringBuilder()
    sb << "DB2_Instance(name: ${db2Instance?.name}, uniqueId: ${db2Instance?.uniqueId})"
}
/******************************Start - Is_Fglams_Required_Upgrade************************************
 /**
 * Initialize the DBSS_Is_Fglams_Required_Upgrade registry flag.
 * This flag will be used to determine if isUpgradeRequired need to be check per agent.
 * Checking isUpgradeRequired in the agent level consider an expensive query
 * we should be limited to the short phase between the upgrade of cartridge
 * till upgrade is complete through the upgrade wizard.
 * otherwise query that flag when not needed will cause performance issues.
 */

def isDBSSFglamsRequiredUpgrade() {
    try {
        def isDBSSFglamsRequiredUpgrade = registryService.getRegistryVariable("DBSS_Is_Fglams_Required_Upgrade");
        return isDBSSFglamsRequiredUpgrade.getGlobalDefault().getDefaultValue();
    } catch (Exception e) {
        //DBSS_Is_Fglams_Required_Upgrade not set assume oracle/db2 cartridge installed
    }

    return -1;
}

def isDBOFglamsRequiredUpgrade() {
    try {
        def isDBOFglamsRequiredUpgrade = registryService.getRegistryVariable("DBO_Is_Fglams_Required_Upgrade");
        return isDBOFglamsRequiredUpgrade.getGlobalDefault().getDefaultValue();
    } catch (Exception e) {
        //DBO_Is_Fglams_Required_Upgrade not set assume sql-server/db2 cartridge installed
    }
    return -1;
}

def isDB2FglamsRequiredUpgrade() {
    try {
        def isDB2FglamsRequiredUpgrade = registryService.getRegistryVariable("DB2_Is_Fglams_Required_Upgrade");
        return isDB2FglamsRequiredUpgrade.getGlobalDefault().getDefaultValue();
    } catch (Exception e) {
        //DB2_Is_Fglams_Required_Upgrade not set assume sql-server/oracle cartridge installed
    }

    return -1;
}

def clearFglamLevelUpgradeRequired() {
    def currentTime = System.currentTimeMillis();
    if (gIsDBSSFglamsRequiredUpgrade != -1 && ((currentTime - gIsDBSSFglamsRequiredUpgrade) > 10l * 60l * 1000l) && !gDBSSAgentsUpgradeNotCompleted) {
        def isDBSSFglamsRequiredUpgrade = registryService.getRegistryVariable("DBSS_Is_Fglams_Required_Upgrade");
        isDBSSFglamsRequiredUpgrade.getGlobalDefault().setDefaultValue(-1l);
        registryService.saveRegistryVariable(isDBSSFglamsRequiredUpgrade);
        println("Clearing DBSS_Is_Fglams_Required_Upgrade setting to -1")
    }

    if (gIsDBOFglamsRequiredUpgrade != -1 && ((currentTime - gIsDBOFglamsRequiredUpgrade) > 10l * 60l * 1000l) && !gDBOAgentsUpgradeNotCompleted) {
        def isDBOFglamsRequiredUpgrade = registryService.getRegistryVariable("DBO_Is_Fglams_Required_Upgrade");
        isDBOFglamsRequiredUpgrade.getGlobalDefault().setDefaultValue(-1l);
        registryService.saveRegistryVariable(isDBOFglamsRequiredUpgrade);
        println("Clearing DBO_Is_Fglams_Required_Upgrade setting to -1")
    }

    if (gIsDB2FglamsRequiredUpgrade != -1 && ((currentTime - gIsDB2FglamsRequiredUpgrade) > 10l * 60l * 1000l) && !gDB2AgentsUpgradeNotCompleted) {
        def isDB2FglamsRequiredUpgrade = registryService.getRegistryVariable("DB2_Is_Fglams_Required_Upgrade");
        isDB2FglamsRequiredUpgrade.getGlobalDefault().setDefaultValue(-1l);
        registryService.saveRegistryVariable(isDB2FglamsRequiredUpgrade);
        println("Clearing DB2_Is_Fglams_Required_Upgrade setting to -1")
    }
}

/******************************End - Is_Fglams_Required_Upgrade************************************/

def createLastHourSpecificTimeRange() {
    def startTime = new Date(System.currentTimeMillis() - 60L * 60L * 1000L); //last 1 hour
    def endTime = new Date(System.currentTimeMillis()); //time to full in ms ahead
    def specificTimeRange = new SpecificTimeRange(startTime, endTime, -1, 0);
    return specificTimeRange;
}

def loadGVLastDataObject() {
    def objGVObs = #!FMS_GV_Data#.getTopologyObjects();
    def fmsGVDataTopology = objGVObs.size == 1 ? objGVObs.get(0) : null;
	if (DEBUG_ENABLED) {
		CAT.debug("loadGVLastDataObject() - fmsGVDataTopology:${topologyBasicToString(fmsGVDataTopology)}.")
	}

    if (fmsGVDataTopology != null) {
		//lastGVObservedValues is with type "List<GV_Observed_Value>"
        def lastGVObservedValues = dataService.retrieveLastNValues(fmsGVDataTopology, "clustersGlobalViewObs", 1);
		if (DEBUG_ENABLED) {
			CAT.debug("loadGVLastDataObject() - lastGVObservedValues?.size():${lastGVObservedValues?.size()}.")
		}
        if (lastGVObservedValues?.size() == 1) {
            def DBWC_GV_GlobalViewDataObj = createEmptyGVData();
			def lastGvClusterDatas = lastGVObservedValues.get(0).getValue()//"lastGvClusterDatas" type is "List<DBWC_GV_GVClusterData>"
			if (DEBUG_ENABLED) {
				CAT.debug("loadGVLastDataObject() - lastGvClusterDatas?.size():${lastGvClusterDatas?.size()}.")
			}
            DBWC_GV_GlobalViewDataObj.set("clustersGlobalView", lastGvClusterDatas);
            return DBWC_GV_GlobalViewDataObj;
        }
    }

    return createEmptyGVData();
}

def createEmptyGVData() {
    type = topologyService.getType("DBWC_GV_GlobalViewRoot");
    obj =  topologyService.createAnonymousDataObject(type);
    return obj
}

/******************************************************************/
/****************************** Main ******************************/
/******************************************************************/

/************************* global variable definition **************************/

// isSQLServerNewVersion is true if we are using at least cartridge version 5.6 . that uses the new agent path.
cartrigeVersion = functionHelper.invokeFunction("system:dbwc.activatedCartridgeVersion","DB_SQL_Server_UI");
isSQLServerNewVersion = functionHelper.invokeFunction("system:dbwc.versionAtLeast56",cartrigeVersion);

// isOracleNewVersion is true if we are using at least cartrige version 5.6 . that uses the new agent path.
cartrigeVersion = functionHelper.invokeFunction("system:dbwc.activatedCartridgeVersion","DB_Oracle_UI");
isOracleNewVersion = functionHelper.invokeFunction("system:dbwc.versionAtLeast56",cartrigeVersion);

// oracleMinimalSCVersion and mssqlMinimalSCVersion are used for pa status
// we call it here so it will be called only one time and not multiple times.
// we use try catch becouse, if the cartrige is not instaled it will fall here.
try{
    oracleMinimalSCVersion = registry("DBO_Minimal_SC_Version");
}catch(e){
    oracleMinimalSCVersion = "0";
}
try{
    mssqlMinimalSCVersion  = registry("DBSS_Minimal_SC_Version");
}catch(e){
    mssqlMinimalSCVersion  = "0";
}

UpgradeCheck.clearCartridgeVersionCache()

def clustersGlobalView = gvData.get("clustersGlobalView");//"clustersGlobalView" is with type "List<DBWC_GV_GVClusterData>"
if (DEBUG_ENABLED) {
	CAT.debug("clustersGlobalView?.size():${clustersGlobalView?.size()}.")
}

def start001  = System.currentTimeMillis();

/* this part fixes the problem when after upgrade, the topology clusterGV dosnt contain isUpgradeRequired property*/
def unUpgraded = clustersGlobalView.find{
    def is_found = false
    try{
        it.get("isUpgradeRequired")
    }catch(e){
        is_found = true
    }

    is_found
}

/* if unUpgraded topologies exist, reset them  and rebuild agents list*/
if(unUpgraded != null) {
    CAT.info("Found un upgraded topologies: resetting clustersGlobalView = [], unUpgraded:${unUpgraded}.")
    clustersGlobalView = []
}

def end001 = System.currentTimeMillis()
def total001 = end001 - start001
if (DEBUG_ENABLED) {
	CAT.debug("Un upgraded topologies check timed total: ${total001}.")
}

//FMS - Is Federation
isFederation = functionHelper.invokeFunction("system:dbwc.123");
if (DEBUG_ENABLED) {
	CAT.debug("isFederation:${isFederation}.")
}

activateUsageFeedbackCollection();

list = [];
gAllAgentMap = getAllAgentMap();
if (gAllAgentMap == null || gAllAgentMap.size() == 0) {
    CAT.warn("gAllAgentMap is Blank. gAllAgentMap:${gAllAgentMap}.")
}

gOSMonitoringState = getOSMonitoringStateMap();
gDBTypeMap = getAllDBTypeMap();
gSeverityTypeMap = getSeverityTypeMap();
gActiveCartridgesList = getActiveCartridgesList();

//def is_vFoglight  = functionHelper.invokeFunction("system:dbwc.255");
gIsVFoglight = functionHelper.invokeFunction("system:dbwc_globalview.422");

//used to prevent unnecessary checking if the agents need to be upgrade after all agents in a Fglam completed a cartridge upgrade
gIsDBSSFglamsRequiredUpgrade = isDBSSFglamsRequiredUpgrade();//
gIsDBOFglamsRequiredUpgrade = isDBOFglamsRequiredUpgrade();//
gIsDB2FglamsRequiredUpgrade = isDB2FglamsRequiredUpgrade();//
gDBSSAgentsUpgradeNotCompleted = false;
gDBOAgentsUpgradeNotCompleted = false;
gDB2AgentsUpgradeNotCompleted = false;


/************** create most updated clusters maps for every cartridge ******************/
// take the cartridges topology mode[relevant instance name : relevant instance]
def mssqlAgents 		= getMSSQLAgents();
def ssrsDatabases       = getReportingServices();
def ssisDatabases       = getSsisDatabases();
def ssasAgents 		= getSSASAgents();
def oracleAgents        = getOracleAgents();
def db2Agents           = getDB2Agents();
def db2Instances        = getAllDB2AraratInstances();
def sqlAzureClusters 	= getSQLAzureInstances();
def sybaseMDAAgents 	= getSybaseMDAAgents()
def sybaseRSAgents  	= getSybaseRSAgents()
def db2LegacyInstances     	  = getDB2LegacyInstances()
def allDB2LegacyMonitorAgents = getAllDB2LegacyMonitorAgents();
def mySQLAgents               = getMySQLAgents();
def postgreSQLAgents          = getPostgreSQLAgents();
def mongoDBAgentsAndClusters  = getMongoDBAgentsAndClusters();
def mongoDBAgents = mongoDBAgentsAndClusters[0];
def mongoDBClusters   = mongoDBAgentsAndClusters[1];
def cassandraAgentsAndClusters   = getCassandraAgents();
def cassandraAgents = cassandraAgentsAndClusters[0]
def cassandraClusters   = cassandraAgentsAndClusters[1];

//def allHostTopologies  		= getAllHostTopologies();
def allHostTopologies  		= null;

uniqueClusterMap = [:];

/**************** getting through all over the existing instances objects **************/
for (int z=0; z < clustersGlobalView.size(); z++) {

    def clusterData = clustersGlobalView.get(z);
    // get the instance topology name from clustersGlobalView
    def currInstance = clusterData.get("topologyObject");

    def relevantInstanceName = extractTopologyKey(currInstance);
    if (relevantInstanceName == null) {
        CAT.warn("extractTopologyKey - relevantInstanceName is null, ignore clustersGlobalView ${clustersGlobalView.name}, currInstance:${topologyBasicToString(currInstance)}.")
        continue;
    }

    // take the current agent tpl from the updated clusters map the leftovers in the map will be added afterwards
    def _sqlAgent = mssqlAgents.remove(relevantInstanceName);

    if (_sqlAgent != null) {
        def _sqlCluster = _sqlAgent.get("cluster");
        def _sqlInstance = _sqlCluster?.get("instances")?.get(0);
        def mssqlClusterData = createMSSQLClusterData(_sqlAgent, _sqlInstance, _sqlCluster, clusterData, allHostTopologies);
        if (mssqlClusterData != null) {
            list.add(mssqlClusterData);
        }
    } else {
        def _agentOra = oracleAgents.remove(relevantInstanceName);
        if (_agentOra != null) {
            def oraClusterData = createOraClusterData(_agentOra, clusterData, allHostTopologies);
            if (oraClusterData != null) {
                list.add(oraClusterData);
            }
        }
        else {
            def _ssrsAgent = ssrsDatabases.remove(relevantInstanceName);
            if (_ssrsAgent != null) {
                def ssrsClusterData = createSsrsClusterData(_ssrsAgent, clusterData);
                if (ssrsClusterData != null) {
                    list.add(ssrsClusterData);
                }
            }
            else {
                def _ssisAgent = ssisDatabases.remove(relevantInstanceName);
                if (_ssisAgent != null) {

                    def ssisClusterData = createSsisClusterData(_ssisAgent, clusterData);
                    if (ssisClusterData != null) {
                        list.add(ssisClusterData);
                    }
                }
                else {
                    def _ssasAgent = ssasAgents.remove(relevantInstanceName);
                    if (_ssasAgent != null) {

                        def ssasClusterData =createSsasClusterData(_ssasAgent, clusterData);
                        if (ssasClusterData != null) {
                            list.add(ssasClusterData);
                        }
                    }
                    else {
                        def _agentRSSybase = sybaseRSAgents.remove(relevantInstanceName)

                        if( _agentRSSybase != null ){
                            list.add(createSybaseRSClusterData(_agentRSSybase, clusterData))
                        }
                        else{
                            def _agentMDASybase = sybaseMDAAgents.remove(relevantInstanceName)

                            if( _agentMDASybase != null ){
                                list.add(createSybaseMDAClusterData(_agentMDASybase, clusterData))
                            }
                            else{
                                def db2LegacyInstance = db2LegacyInstances.remove(relevantInstanceName);
                                if( db2LegacyInstance != null ){
                                    list.add(createDB2LegacyClusterData(db2LegacyInstance, clusterData, allDB2LegacyMonitorAgents))
                                }
                                else{
                                    def sqlAzureInstance = sqlAzureClusters.remove(relevantInstanceName);
                                    if( sqlAzureInstance != null ){
                                        list.add(createMssqlAzureClusterData(sqlAzureInstance, clusterData));
                                    }
                                    else {
                                        def db2agent = db2Agents.remove(relevantInstanceName);
                                        if( db2agent != null ){
                                            def db2ClusterData = createDB2ClusterData(db2agent, db2Instances, clusterData);
                                            if (db2ClusterData != null) {
                                                list.add(db2ClusterData);
                                            }
                                        }
                                        else {
                                            def mySQLagent = mySQLAgents.remove(relevantInstanceName);
                                            if( mySQLagent != null ){
                                                def mySQLClusterData = createMySQLClusterData(mySQLagent, mySQLagent, clusterData);
                                                if (mySQLClusterData != null) {
                                                    list.add(mySQLClusterData);
                                                }
                                            }
                                            else {
                                                def postgreSQLagent = postgreSQLAgents.remove(relevantInstanceName);
                                                if( postgreSQLagent != null ){
                                                    def postgreSQLData = createPostgreSQLData(postgreSQLagent, postgreSQLagent, clusterData);
                                                    if (postgreSQLData != null) {
                                                        list.add(postgreSQLData);
                                                    }
                                                } else {
                                                    def mongoDBagent = mongoDBAgents.remove(relevantInstanceName);
                                                    if( mongoDBagent != null ){
                                                        def mongoDBCluster = mongoDBClusters.get(buildDBAgentKey(mongoDBagent));
                                                        if (mongoDBCluster == null) {
                                                            printDebugModeLog("Failed to get the mongoDBCluster for agent => " + buildDBAgentKey(mongoDBagent), null);
                                                            continue;
                                                        }
                                                        def mongoDBData = createMongoTopLevelData(mongoDBagent, mongoDBCluster, clusterData);
                                                        if (mongoDBData != null) {
                                                            list.add(mongoDBData);
                                                        }
                                                    } else {
                                                        def cassandraAgent = cassandraAgents.remove(relevantInstanceName);
                                                        if( cassandraAgent != null ){
                                                            def cassandraCluster = cassandraClusters.get(buildDBAgentKey(cassandraAgent));
                                                            def cassandraData = createCassandraClustersData(cassandraAgent, cassandraCluster, clusterData);
                                                            if (cassandraData != null) {
                                                                list.add(cassandraData);
                                                            }
                                                        } else {
                                                            /* In case we got to here, the object isn't exist and he could not be found */
                                                            printDebugModeLog ("New agent topology could not be found for: " + relevantInstanceName, null);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/*************************** add new instances to displayed list ************************/
String db2AgentName = "";
for (db2agent in db2Agents.values()) {
    db2AgentName = "unknown";
    try {
        db2AgentName = db2agent.get("name");
        if (db2AgentName != null && !db2AgentName.equals("null")) {
            def db2ClusterData = createDB2ClusterData(db2agent, db2Instances, null);
            if (db2ClusterData != null) {
                list.add(db2ClusterData);
            }
        }
    } catch (Exception e) {
        println "Failed to create GV Data for DB2 instance agent name - " + db2AgentName + "\n" + e;
    }
}


String sqlAzureInstanceName = "";
for (azureInstance in sqlAzureClusters.values()) {
    sqlAzureInstanceName = "unknown";
    try {
        sqlAzureInstanceName = azureInstance.get("name");
        if (sqlAzureInstanceName != null && !sqlAzureInstanceName.equals("null")) {
            list.add(createMssqlAzureClusterData(azureInstance, null));
        }
    } catch (Exception e) {
        println "Failed to create GV Data for Azure instance agent name - " + sqlAzureInstanceName + "\n" + e
    }
}


String db2LegacyInstanceName = "";
for (db2LegacyInstance in db2LegacyInstances.values()) {
    db2LegacyInstanceName = "unknown";
    try {
        db2LegacyInstanceName = db2LegacyInstance.get("name");
        if (db2LegacyInstanceName != null && !db2LegacyInstanceName.equals("null")) {
            list.add(createDB2LegacyClusterData(db2LegacyInstance, null, allDB2LegacyMonitorAgents));
        }
    } catch (Exception e) {
        println "Failed to create GV Data for DB2 legacy agent name - " + db2LegacyInstanceName + "\n" + e
    }
}

String sybaseMdaAgentName = "";
for (sybaseAgent in sybaseMDAAgents.values()) {

    sybaseMdaAgentName = "unknown";
    try {
        sybaseMdaAgentName = sybaseAgent.get("name");
        if (sybaseMdaAgentName != null && !sybaseMdaAgentName.equals("null")) {
            list.add(createSybaseMDAClusterData(sybaseAgent, null));
        }
    } catch (Exception e) {
        println "Failed to create GV Data for Sybase MDA agent name - " + sybaseMdaAgentName + "\n" + e
    }
}

String sybaseRsAgentName = "";
for (sybaseAgent in sybaseRSAgents.values()) {

    sybaseRsAgentName = "unknown";
    try {
        sybaseRsAgentName = sybaseAgent.get("name");
        if (sybaseRsAgentName != null && !sybaseRsAgentName.equals("null")) {
            list.add(createSybaseRSClusterData(sybaseAgent, null));
        }
    } catch (Exception e) {
        println "Failed to create GV Data for Sybase RS agent name - " + sybaseRsAgentName + "\n" + e
    }
}

String ssrsName = "";
for (ssrsDatabase in ssrsDatabases.values()) {
    try {
        ssrsName = ssrsDatabase?.get("name");
        def ssrsClusterData = createSsrsClusterData(ssrsDatabase, null);
        if (ssrsClusterData != null) {
            list.add(ssrsClusterData);
        }
    } catch (Exception e) {
        printDebugModeLog ("Failed to create GV Data for ssrs database - " + ssrsName + "\n", e) ;
    }
}

String ssisName = "";
for (ssisDatabase in ssisDatabases.values()) {
    try {
        ssisName = ssisDatabase?.get("name");
        def ssisClusterData = createSsisClusterData(ssisDatabase, null);
        if (ssisClusterData != null) {
            list.add(ssisClusterData);
        }
    } catch (Exception e) {
        printDebugModeLog ("Failed to create GV Data for ssis database - " + ssisName + "\n", e) ;
    }
}

String ssasName = "";
for (ssasAgent in ssasAgents.values()) {
    try {
        ssasName = ssasAgent?.get("name");
        def ssasClusterData = createSsasClusterData(ssasAgent, null);
        if (ssasClusterData != null) {
            list.add(ssasClusterData);
        }
    } catch (Exception e) {
        printDebugModeLog ("Failed to create GV Data for ssis database - " + ssasName + "\n", e) ;
    }
}

String mssqlAgentName = "";
for (_agentSql in mssqlAgents.values()) {
    def _clusterSql = _agentSql.get("cluster");
    mssqlAgentName = "unknown";
    try {
        mssqlAgentName = _agentSql?.get("name");
        if (mssqlAgentName != null && !mssqlAgentName.equals("null")) {
            def _instanceSql = _clusterSql?.get("instances")?.get(0);
            def mssqlClusterData = createMSSQLClusterData(_agentSql, _instanceSql, _clusterSql, null, allHostTopologies);
            if (mssqlClusterData != null) {
                list.add(mssqlClusterData);
            }
        }
        else {
            printDebugModeLog ("Failed to get SQL Server cluster data of _clusterSql = " + _clusterSql, null);
        }
    } catch (Exception e) {
        printDebugModeLog ("Failed to create 'DBSS_Data_Model' for agent name - " + mssqlAgentName + "\n", e) ;
    }
}

String oracleAgentName = "";
for (_agentOra in oracleAgents.values()) {
    oracleAgentName = "unknown";
    try {
        oracleAgentName = _agentOra.oracleAgent.get("name");
        if (oracleAgentName != null && !oracleAgentName.equals("null")) {
            def dboData = createOraClusterData(_agentOra, null, allHostTopologies);
            if (dboData != null) {
                list.add(dboData);
            }
        }
    } catch (Exception e) {
        printDebugModeLog ("Failed to create 'DBO_Data_Model' for agent name - " + oracleAgentName + "\n",e);
    }
}

for (def mongoDBAgent in mongoDBAgents.values()) {
    String mongoDBAgentName = "unknown";
    try {
        mongoDBAgentName = mongoDBAgent?.get("name");
        if (mongoDBAgentName != null && !mongoDBAgentName.equals("null")) {
            def mongoDBCluster = mongoDBClusters.get(buildDBAgentKey(mongoDBAgent));
            if (mongoDBCluster == null) {
                printDebugModeLog("Failed to get the mongoDBCluster/mongoDBReplSet for agent => " + buildDBAgentKey(mongoDBAgent), null);
                continue;
            }
            def mongoDBData = createMongoTopLevelData(mongoDBAgent, mongoDBCluster, null);
            if (mongoDBData != null) {
                list.add(mongoDBData);
            }
        }
    } catch (Exception e) {
        printDebugModeLog ("Failed to create 'MongoDB_Data_Model' for agent name - " + mongoDBAgentName + "\n",e);
    }
}
String cassandraAgentName = "";
for (cassandraAgent in cassandraAgents.values()) {
    cassandraAgentName = "unknown";
    try {
        cassandraAgentName = cassandraAgent?.get("name");
        if (cassandraAgentName != null && !cassandraAgentName.equals("null")) {
            def cassandraCluster = cassandraClusters.get(buildDBAgentKey(cassandraAgent));
            def cassandraData = createCassandraClustersData(cassandraAgent, cassandraCluster, null);
            if (cassandraData != null) {
                list.add(cassandraData);
            }
        }
    } catch (Exception e) {
        printDebugModeLog ("Failed to create 'Cassandra_Data_Model' for agent name - " + cassandraAgentName + "\n",e);
    }
}
String mySQLAgentName = "";
for (_agentMySQL in mySQLAgents.values()) {
    mySQLAgentName = "unknown";
    try {
        mySQLAgentName = _agentMySQL?.get("name");
        if (mySQLAgentName != null && !mySQLAgentName.equals("null")) {
            def mySQLData = createMySQLClusterData(_agentMySQL, _agentMySQL, null);
            if (mySQLData != null) {
                list.add(mySQLData);
            }
        }
    } catch (Exception e) {
        printDebugModeLog ("Failed to create 'mySQL_Data_Model' for agent name - " + mySQLAgentName + "\n",e);
    }
}

String postgreSQLAgentName = "";
for (_agentPostgreSQL in postgreSQLAgents.values()) {
    postgreSQLAgentName = "unknown";
    try {
        postgreSQLAgentName = _agentPostgreSQL?.get("name");
        if (postgreSQLAgentName != null && !postgreSQLAgentName.equals("null")) {
            def postgreSQLData = createPostgreSQLData(_agentPostgreSQL, _agentPostgreSQL, null);
            if (postgreSQLData != null) {
                list.add(postgreSQLData);
            }
        }
    } catch (Exception e) {
        printDebugModeLog ("Failed to create 'postgressSQL_Data_Model' for agent name - " + postgreSQLAgentName + "\n",e);
    }
}

/********************** Push global data to GV_Data_Model **************************/

def gvDataModelTopologies = #!GV_Data_Model#.getTopologyObjects();
def gvDataModelTopology;
// Create root level GV_Data_Model if not exist
if (gvDataModelTopologies.size() == 0) {
    def gvDataModelTypeObj = topologyService.getObjectShell(topologyService.getType("GV_Data_Model"));
    gvDataModelTypeObj.set("name", "GV Data Model");
    gvDataModelTopology = topologyService.mergeData(gvDataModelTypeObj);
    println("Building GV_Data_Model first entry");
} else {
    gvDataModelTopology = gvDataModelTopologies[0];
}

def fmsGvData = gvDataModelTopology.get("fmsGVData");

if (fmsGvData == null) {
    def fmsGVDataModelTypeObj = topologyService.getObjectShell(topologyService.getType("FMS_GV_Data"));
    fmsGVDataModelTypeObj.set("name", "FMS GV Data");
    fmsGvData = topologyService.mergeData(fmsGVDataModelTypeObj);
    gvDataModelTopology = topologyService.beginUpdate(gvDataModelTopology);
    gvDataModelTopology.set("fmsGVData", fmsGvData);
    gvDataModelTopology = topologyService.endUpdate(gvDataModelTopology);
    fmsGvData = gvDataModelTopology.get("fmsGVData");
}

def gvGlobalViewRootResult = createEmptyGVData()//Type "DBWC_GV_GlobalViewRoot"

// publish the observation data object
def sampledPeriod = 60l * 60l * 1000l;
if (list != null && list.size() > 0) {
    def node = server.CanonicalFactory.createNode(fmsGvData, "clustersGlobalViewObs", list, sampledPeriod);
    dataService.publish(node);
    gvGlobalViewRootResult.set("clustersGlobalView", list);
} else {
    if (DEBUG_ENABLED) {
        CAT.debug("loadGVData() - empty gvClusterData list occurred. gvClusterData list:${list}.")
    }
    if (list != null) {
        def node = server.CanonicalFactory.createNode(fmsGvData, "clustersGlobalViewObs", list, sampledPeriod);
        dataService.publish(node);
        gvGlobalViewRootResult.set("clustersGlobalView", list);
    }
}

// clear Is_Fglams_Required_Upgrade registry flag if all agents completed upgrade
clearFglamLevelUpgradeRequired();

if (DEBUG_ENABLED) {
    CAT.debug("loadGVData() - exit. gvClusterDatas size:${gvGlobalViewRootResult?.clustersGlobalView?.size()}, spentTime:${stopWatch.getTime()}ms.")
}
return gvGlobalViewRootResult;
