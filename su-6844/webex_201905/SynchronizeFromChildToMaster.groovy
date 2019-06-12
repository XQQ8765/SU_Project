/**
 * The script should run in the federation master FMS.
 * The purpose for this script is synchronized specific types' topologyobjects from FMS child to master.
 * It may resolve the topologyobject synchronize problem between federation master and child.
 * See wiki: https://wiki.labs.quest.com/display/FDM/The+workaround+of+Federation+synchronization+issue
 */
import com.quest.nitro.model.topology.TopologyType;
import  com.quest.nitro.model.topology.TopologyObject;
import com.quest.nitro.model.topology.TopologyProperty;
import com.quest.wcf.data.wcfdo.Property;

mLocalTopologyService = server.TopologyService;
PROP_TOPOLOGYOBJECT_SOURCEIDS = "sourceIds";

//Fill in topologyType(s) which needs to synchronize
def typeNames = ['Fill in the target topologyType which needs to synchronize', '', ''];

for(def tn:typeNames){
	def t = mLocalTopologyService.getType(tn);
	def objs = mLocalTopologyService.getObjectsOfType(t);

	for(def object: objs){
		TopologyType type = object.getType();
		TopologyObject shell = mLocalTopologyService.getObjectShell(type);
		for (TopologyProperty p : type.getIdentityProperties()) {
			shell.set(p, object.get(p));
		}
		Property srcIdsProp = type.getProperty(PROP_TOPOLOGYOBJECT_SOURCEIDS);
		shell.getList(srcIdsProp).clear();
		//shell.getList(srcIdsProp).addAll(keptIds);
		shell.markOverridingProperty((TopologyProperty) srcIdsProp);

		mLocalTopologyService.mergeData(shell);
	}
}