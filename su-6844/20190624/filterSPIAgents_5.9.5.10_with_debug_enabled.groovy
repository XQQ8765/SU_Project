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
package system._dbwc_globalview_spiglobalview.scripts;
import com.quest.qsi.fason.framework.utils.*

/**
 * This function filter the instances by cartridge version and pa status. So that the instances will be show in "Databases - SQL PI" dashboard.
 * Required:
 *  allAgents - List of DBWC_GV_GVClusterData
 * Return Value:
 *     List DBWC_GV_GVClusterData
 */

class EnableDebugLog4JLogger extends org.apache.commons.logging.impl.Log4JLogger {
    EnableDebugLog4JLogger(name) {
        super(name)
    }

    public boolean isDebugEnabled() {
        return true
    }
    public void debug(Object message) {
        info("DEBUG: ${message}")
    }

    public boolean isTraceEnabled() {
        return true
    }
    public void trace(Object message) {
        info("TRACE: ${message}")
    }
}
CAT = new EnableDebugLog4JLogger("script.dbwc_globalview_spiglobalview.filterSPIAgents.su_6844")
//CAT = com.quest.qsi.fason.framework.fmslogger.LogFactory.getLogForWCF(functionHelper.getFunctionId())
DEBUG_ENABLED = CAT.isDebugEnabled()

//Define constant variable to the determine from which version we support agent that configure with SPI
DEF_SQL_SERVER_MIN_SPI_VERSION = new Version("5.7.0.1")
DEF_ORACLE_MIN_SPI_VERSION     = new Version("5.7.5.1")

//Parameters that specified whether SPI agent should display
gIsSupportSQLServerSPI=false;
gIsSupportOracleSPI=false;


/*
  Get the activate cartridge version
  Return Value:
     Version object - for the activate cartridge
	 Version (0) - In case the function failed or the cartridge does not exist
*/
def getActivateCartridgeVersion(cartName) {
    def cartVersionStr = functionHelper.invokeFunction("system:dbwc.activatedCartridgeVersion", cartName);
    if (cartVersionStr == null) {
        cartVersionStr = "0";
    }
    return new Version(cartVersionStr)
}

/**
 Initialize global parameters
 **/
def init() {
    gIsSupportSQLServerSPI = getActivateCartridgeVersion("DB_SQL_Server_UI").compareTo(DEF_SQL_SERVER_MIN_SPI_VERSION) >=0;
    gIsSupportOracleSPI = getActivateCartridgeVersion("DB_Oracle_UI").compareTo(DEF_ORACLE_MIN_SPI_VERSION) >=0;
}

/**
 * Check whether the gvClusterData is configure as SPI
 * Return Value:
 *   true   - Allow add an SPI agent to the list
 *   false -  Do not allow to add an SPI agent to the list
 * Note: Currently only Oracle and SQL server support SPI agent
 **/
def isAllowToAddGVCluster(gvClusterData)  {//"gvClusterData" type is: DBWC_GV_GVClusterData
    def agentType = gvClusterData.get("agent").get("type");
    def paState = gvClusterData.get("paState");
    def allowToAddAgentForSPI = false
    if ( ((gIsSupportSQLServerSPI) && (agentType == "DB_SQL_Server"))
            || (gIsSupportOracleSPI && (agentType in ["DB_Oracle", "DB_Oracle_RAC", "DB_Oracle_RAC_Instance"])) ) {
        allowToAddAgentForSPI = (paState != null && paState>10);
    }
    return allowToAddAgentForSPI
}

/***************************************************************************************/
/*                                     MAIN                                            */
/***************************************************************************************/
init();
if (DEBUG_ENABLED) {
    CAT.debug("filterSPIAgents() enter. allAgents?.size():${allAgents?.size()}, gIsSupportSQLServerSPI:${gIsSupportSQLServerSPI}, gIsSupportOracleSPI:${gIsSupportOracleSPI}.")
}
def spiResults = [];
try {
    //Scan all the agent.
    for(gvClusterData in allAgents) {
        if (isAllowToAddGVCluster(gvClusterData)) {
            spiResults.add(gvClusterData);
        }
    }
} catch(e) {
    println("Error executing script." + functionHelper.getFunctionId() + ". Error: " + e.getMessage());
    e.printStackTrace();
}

if (DEBUG_ENABLED) {
    CAT.debug("filterSPIAgents() exit. allAgents?.size():${allAgents?.size()}, spiResults?.size():${spiResults?.size()}.")
}

return spiResults;
