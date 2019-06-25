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

import com.quest.wcf.servicelayer.*;
import org.apache.commons.logging.Log;
import org.apache.commons.lang.StringUtils;

/**
 * The function is "#Global View - get all node entities"(system:dbwc.34), the code version is:5.7.5.50.
 * We have added some additional log here
 * @param gvData - DBWC_GV_GlobalViewRoot
 * @return 	DBWC_GV_GlobalViewRoot
 */
//log = org.apache.commons.logging.LogFactory.getLog("script." + functionHelper.getFunctionId());
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
log = new EnableDebugLog4JLogger("script.dbwc.34.su_6844_v2")
DEBUG_ENABLED = log.isDebugEnabled()

//################# Global Definition ######################//

//fms services declarations
topologyService = server.get("TopologyService");
dataService = server.get("DataService");
securityService = server.get("SecurityService");

userName = securityService.getCurrentPrincipal().getUsername();
groupNames = getUserGroupNames(userName);
gIsFederation = functionHelper.invokeFunction("system:dbwc.123");

refershIntervalWhileUserOnScreenInSec = 600;


//################## Function Definition ##################//

/**
 * Build empty global view data structure
 * @return
 */
def createEmptyGVData() {
    type = topologyService.getType("DBWC_GV_GlobalViewRoot");
    obj = topologyService.createAnonymousDataObject(type);
    return obj
}

/**
 * Reload gv data async in order to maintain up to date data while user is on the screen
 */
def reloadGVDataAsync() {
    try {
        if (DEBUG_ENABLED) {
            log.debug("reloadGVDataAsync() - enter.")
        }
        new Thread(new Runnable() {
            void run() {
                def invokeFunctionTimeRange = null;
                def invokeFunctionArg2 = null;
                if (DEBUG_ENABLED) {
                    log.debug("reloadGVDataAsync() - in Thread.run(): before invoke function \"system:dbwc.loadGVData\".")
                }
                def loadGVData = ServiceRegistry.getEvaluationService().invokeFunction("system:dbwc.loadGVData", [], invokeFunctionTimeRange, invokeFunctionArg2);
                if (DEBUG_ENABLED) {
                    log.debug("reloadGVDataAsync() - in Thread.run(): after invoke function \"system:dbwc.loadGVData\".")
                }
            }
        }).start();
    } catch (Exception e) {
        log.info("unable to load the global view data on demand " + e.getMessage());
    }
}

/**
 * Load the global view data structure
 * @return DBWC_GV_GlobalViewRoot
 */
def loadGVLastDataObject() {
    def objGVObs = #!FMS_GV_Data#.getTopologyObjects();
    def dbObservationTopology = objGVObs.size == 1 ? objGVObs.get(0) : null;
    if (DEBUG_ENABLED) {
        log.debug("loadGVLastDataObject() - dbObservationTopology:${dbObservationTopology}.")
    }

    def gvData;
    if (dbObservationTopology != null) {
        def lastGVValue = dataService.retrieveLastNValues(dbObservationTopology, "clustersGlobalViewObs", 1);//type: GV_Observed_Value
        if (DEBUG_ENABLED) {
            log.debug("loadGVLastDataObject() - lastGVValue?.size():${lastGVValue?.size()}.")
        }
        if (lastGVValue.size() == 1) {
            def DBWC_GV_GlobalViewDataObj = createEmptyGVData();//Type "DBWC_GV_GlobalViewRoot"
            def lastClustersGlobalViewData = lastGVValue.get(0)//Type "GV_Observed_Value"

            if (DEBUG_ENABLED) {
                log.debug("loadGVLastDataObject() - lastClustersGlobalViewData:${lastClustersGlobalViewData}.")
            }

            //refresh the GV data in the following cases:
            //  1. Non federation FMS
            //  2. GV data updated less than specified interval in case the FMS is a federator (enable the customer still to have up to date while the user is on the screen)
            def gvDataLastUpdateTime = lastClustersGlobalViewData.getEndTime().getTime();
            def needToBeUpdatedByTime = isGVDataNeedToBeUpdatedByTime(gvDataLastUpdateTime)
            if (DEBUG_ENABLED) {
                log.debug("reloadGVDataAsync() - gIsFederation:${gIsFederation}, endTime:${lastClustersGlobalViewData.getEndTime()}, gvDataLastUpdateTime:${gvDataLastUpdateTime}, needToBeUpdatedByTime:${needToBeUpdatedByTime}.")
            }
            def needToReloadGVAsync = !gIsFederation || needToBeUpdatedByTime
            if (!needToReloadGVAsync) {
                needToReloadGVAsync = needToReloadGVInstancesOrNot(lastClustersGlobalViewData)
            }
            if (DEBUG_ENABLED) {
                log.debug("reloadGVDataAsync() - needToReloadGVAsync:${needToReloadGVAsync}.")
            }
            if (needToReloadGVAsync) {
                reloadGVDataAsync();
            }

            if (DEBUG_ENABLED) {
                log.debug("loadGVLastDataObject() - lastClustersGlobalViewData.getValue():${lastClustersGlobalViewData?.getValue()}.")
            }
            DBWC_GV_GlobalViewDataObj.set("clustersGlobalView", lastClustersGlobalViewData.getValue());
            gvData = DBWC_GV_GlobalViewDataObj;
        } else {
            log.info("Unable to retrieve global view data from the topology, building GV data structure on demand");
            //loadGVData return DBWC_GV_GlobalViewRoot
            def loadGVData = ServiceRegistry.getEvaluationService().invokeFunction("system:dbwc.loadGVData", [], null, null);
            gvData = loadGVData
        }
    } else {
        //FMS_GV_Data topology have not yet been created - occur only on first install
        //The flow should only called once otherwise the global view page load could be delayed
        log.info("first entrance to the global view page after cartridge install");
        //loadGVData return DBWC_GV_GlobalViewRoot
        def loadGVData = ServiceRegistry.getEvaluationService().invokeFunction("system:dbwc.loadGVData", [], null, null);
        gvData = loadGVData
    }

    //apply user management filtering
    def returnResult = filterMonitoredInstancesForUser(gvData);
    if (DEBUG_ENABLED) {
        log.debug("loadGVLastDataObject() - returnResult:${returnResult}.")
    }
    return returnResult;
}

/**
 * Filter global view monitored instances data for current user
 * @param gvData all global view monitored instance data
 * @return gvData - DBWC_GV_GlobalViewRoot, filtered for current user
 */
def filterMonitoredInstancesForUser(gvData) {

    def filteredGvData = [];

    def userFilter = functionHelper.invokeFunction("system:dbwc_globalview_user20level20login.isAllAgentsAssociatedWithUser2", userName, groupNames, userConfiguration, groupEcMap);

    if (userFilter.equals("None")) {
        gvData.set("clustersGlobalView", filteredGvData);
        return gvData;
    } else if (userFilter.equals("All")) {
        return gvData;
    }

    def gvDataClusters = gvData.get("clustersGlobalView");
    for (clusterData in gvDataClusters) {
        def agent = clusterData?.get("agent");
        def agentType = clusterData?.get("clusterIdentifier")?.get("databaseType")?.get("value");
        def agentName = clusterData?.get("clusterIdentifier")?.get("agentName");
        if (isAgentAssociatedToUser(agent, agentType, agentName)) {
            filteredGvData.add(clusterData);
        }
    }

    gvData.set("clustersGlobalView", filteredGvData);
    return gvData;
}


/**
 * Verify if agent associated to user
 * @param agent the agent for getting name
 * @param agentType refer to DBWC_GV_DatabaseTypeEnum
 * @param agentName agent name
 * @return true if agent associated to user
 */
def isAgentAssociatedToUser(agent, agentType, agentName) {

    if (agent == null) {
        log.info("Invalid agent");
        return true;
    }

    def instanceName = agent.getName();

    if (agentType == null) {
        log.info("Invalid agent type for agent: \"" + instanceName + "\"");
    }

    def instanceType;
    switch (agentType) {
        case "Oracle": instanceType = "Oracle"; break;
        case "SSIS": instanceName = agentName;  instanceType = "SSIS"; break;
        case "SSRS": instanceName = agentName;  instanceType = "SSRS"; break;
        case "SSAS": instanceName = agentName;  instanceType = "SSAS"; break;
        case "MSSQL": instanceName = agentName; instanceType = "SQL Server"; break;
        case "SYBASE":
            instanceName = agent.get("agentInstance");//workaround to sybase since agent name is not constructed properly
            instanceType = "Sybase"; break;
        case "DB_DB2":
        case "DB2": instanceType = "DB2"; break;
        case "MySQL": instanceType = "MySQL"; break;
        case "MongoDB": instanceType = "MongoDB"; break;
        case "Cassandra": instanceType = "Cassandra"; break;
        case "PostgreSQL": instanceType="PostgreSQL"; break;
        case "AZURE": return true;
        default:
            log.info("Unknown agentType \""+ agentType + "\". Do not filter agent \""+ instanceName + "\"");
            return true;

    }


    return functionHelper.invokeFunction("system:dbwc_globalview_user20level20login.isAgentAssociatedWithUser2", userName, instanceName, instanceType, groupNames, userConfiguration, groupEcMap);
}

def isDBInstanceAgent(agent) {//agent Type is Topology type: "Agent"
    if (agent == null) {
        return false
    }
    def isDBDomainAgent = StringUtils.startsWith(agent.topologyController?.fullyQualifiedName, "Database.")
    if (!isDBDomainAgent) {
        return false
    }
    def isInstallerAgent = StringUtils.contains(agent.topologyTypeName, 'Installer_Agent_Model')
    return !isInstallerAgent//Skip DB Installer agent
}

def needToReloadGVInstancesOrNot(lastGVObservedValue) {//Type "GV_Observed_Value"
    def clusterDatasInGVIsEmpty = false
    def clusterDatas = lastGVObservedValue?.getValue()//"clusterDatas" type is "List<DBWC_GV_GVClusterData>"
    if (clusterDatas == null || clusterDatas.size() == 0) {
        clusterDatasInGVIsEmpty = true
    }
    def agents = #!Agent#.getTopologyObjects()
    def dbAgentExists = agents?.any { agent ->
        return isDBInstanceAgent(agent)
    }
    if (DEBUG_ENABLED) {
        log.debug("needToReloadGVInstancesOrNot() - clusterDatasInGVIsEmpty:${clusterDatasInGVIsEmpty}, dbAgentExists:${dbAgentExists}.")
    }
    //To Fix SU-6844, if the "instances" in GV last "GV_Observed_Value" is empty, but DB Agents exists, then we need to reload "GV" data.
    return (clusterDatasInGVIsEmpty && dbAgentExists)
}

def isGVDataNeedToBeUpdatedByTime(gvDataLastUpdateTime) {
    return (System.currentTimeMillis() - gvDataLastUpdateTime) / 1000 > refershIntervalWhileUserOnScreenInSec;
}

def getUserGroupNames(userName) {
    def securityService = server.get("SecurityService");
    def secUser = securityService.getUserByName(userName);
    def secGroups = secUser.getGroups();


    def allGroups = securityService.findAllGroups();
    def allGroupsNames = [];
    for (allGroup in allGroups) {
        allGroupsNames.add(allGroup.getGroupName());
    }

    def groupNames = [];
    for (secGroup in secGroups) {
        gName = secGroup.getGroupName();
        if (allGroupsNames.contains(gName)) {
            groupNames.add(gName);
        }
    }
    return groupNames;
}

def getEntityConfiguration(entityName, entityType) {
    def query = "DBWC_GV_UserLevelLogin where user = '" + entityName + "' and entityType = '" + entityType + "'";
    topologies = queryService.queryTopologyObjects(query);

    if (topologies != null && topologies.size() > 0) {
        return topologies.toList().get(0);
    }


    return null;

}

def getGroupEntityConfigurationMap() {
    def groupEntityConfs = [:];
    def query = "DBWC_GV_UserLevelLogin where entityType = 'group'";
    topologies = queryService.queryTopologyObjects(query);

    if (topologies != null) {
        topologies.each{ groupConf ->
            if (groupEntityConfs.get(groupConf.user) == null) {
                groupEntityConfs.get(groupConf.user, groupConf) ;
            }
        }
    }


    return groupEntityConfs;

}

//################## Main Script Flow ##################//
if (DEBUG_ENABLED) {
    log.debug("dbwc.34() - enter.")
}
queryService = server.get("QueryService");

userConfiguration = getEntityConfiguration(userName, "user");
groupEcMap = getGroupEntityConfigurationMap();
//return the global view data to the wcf view
def result = loadGVLastDataObject();//result type is "DBWC_GV_GlobalViewRoot"
//populate the gvData to be used for as context data structure such as the quick view
gvData.set("clustersGlobalView", result.get("clustersGlobalView"));
if (DEBUG_ENABLED) {
    log.debug("dbwc.34() - exit. result?.clustersGlobalView?.size():${result?.clustersGlobalView?.size()}.")
}
return result;//DBWC_GV_GlobalViewRoot