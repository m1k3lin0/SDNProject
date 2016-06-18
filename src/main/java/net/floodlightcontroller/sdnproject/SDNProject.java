/*
 * main module 
 */
package net.floodlightcontroller.sdnproject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowMod;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.action.OFActions;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.ver13.OFFactoryVer13;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IPv6Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TransportPort;
import org.projectfloodlight.openflow.types.U64;
import org.projectfloodlight.openflow.types.VlanVid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.LLDP;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.packet.UDP;
import net.floodlightcontroller.restserver.IRestApiService;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.sdnproject.web.SDNProjectRoutable;
import net.floodlightcontroller.staticflowentry.IStaticFlowEntryPusherService;
import net.floodlightcontroller.statistics.web.SwitchPortBandwidthSerializer;
import net.floodlightcontroller.storage.CompoundPredicate;
import net.floodlightcontroller.storage.IPredicate;
import net.floodlightcontroller.storage.IResultSet;
import net.floodlightcontroller.storage.IStorageSourceListener;
import net.floodlightcontroller.storage.IStorageSourceService;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.topology.NodePortTuple;
import net.floodlightcontroller.storage.OperatorPredicate;
import net.floodlightcontroller.storage.OperatorPredicate.Operator;
import net.floodlightcontroller.util.FlowModUtils;
import net.floodlightcontroller.util.OFMessageDamper;

public class SDNProject implements IOFMessageListener, IFloodlightModule, IStorageSourceListener {

	protected static Logger log = LoggerFactory.getLogger(SDNProject.class);

	/* network parameters */
	// TODO count the available servers, make private and implement method get and update?
	protected static final int FANOUT = 2;
	protected static final int DEPTH = 4;
	protected static final int tot_servers = (int)Math.pow(FANOUT, DEPTH);
	public static int available_servers;

	
	public static final String FIRST_VIRTUAL_ADDR = "192.168.";
	public static final String FIRST_PHYSICAL_ADDR = "10.0.";

	/* module constant */
	
	// table of servers
	public static final String TABLE_SERVERS 	= "SDNProject_servers";
	public static final String COLUMN_S_ID		= "ID";
	public static final String COLUMN_S_PHYSICAL= "physical_address";
	public static final String COLUMN_S_VIRTUAL = "virtual_address";
	public static final String COLUMN_S_USER 	= "user";
	
	// table of the users
	public static final String TABLE_USERS		= "SDNProject_users";
	public static final String COLUMN_U_NAME 	= "user";
	public static final String COLUMN_U_SERVERS = "servers";
	
	// table of the rules
	public static final String TABLE_RULES		= "SDNProject_rules";
	public static final String COLUMN_R_NAME	= "rule_name";
	public static final String COLUMN_R_SRC		= "source_address";
	public static final String COLUMN_R_DST		= "destination_address";
	
	/* services */
	protected IRestApiService restAPIService;
	protected IStorageSourceService storageSourceService;
	protected IFloodlightProviderService floodlightProviderService;
	protected IStaticFlowEntryPusherService staticFlowEntryPusherService;
	protected IRoutingService routingService;
	protected IDeviceService deviceService;
	protected ITopologyService topologyService;
	protected IOFSwitchService switchService;

	//****************************************************
	
	/**
	 * finds all the switches on the path that connect two devices identified by
	 * the provided IP addresses
	 * @param sourceAddress is the IP address of the source host
	 * @param destAddress is the IP address of the destination host
	 * @return a list of all the switches on the path as List of DatapathId
	 * */
	private List<DatapathId> getSwitches(String sourceAddress, String destAddress) {
		
		List<DatapathId> switches = new ArrayList<DatapathId>();
		
		Set<DatapathId> allSwitches = switchService.getAllSwitchDpids(); //get all the switches in the topology
		List<DatapathId> edgeSwitches = new ArrayList<DatapathId>();
		for (DatapathId sw : allSwitches) {
			if(topologyService.isEdge(sw, OFPort.of(1))){ //it is sufficient to check only the port 1
				edgeSwitches.add(sw);
			}
		}
		Collections.sort(edgeSwitches); //sort
		
		int index = (IPv4Address.of(sourceAddress).getInt()-IPv4Address.of(FIRST_PHYSICAL_ADDR+"0.1").getInt())/FANOUT;
		DatapathId srcSwitch = edgeSwitches.get(index); //DatapathId.of("00:00:00:00:00:00:00:02");
		index = (IPv4Address.of(destAddress).getInt()-IPv4Address.of(FIRST_PHYSICAL_ADDR+"0.1").getInt())/FANOUT;
		DatapathId dstSwitch = edgeSwitches.get(index);
		
		
		
		//if they are equal, there's no need to compute the route because the switch is just one
		if(srcSwitch.equals(dstSwitch)){
			switches.add(srcSwitch);
			return switches;
		}
		
		// take the switches traversed by the path between source and destination
		Route route = null;
		
		try {
			route = routingService.getRoute(srcSwitch, dstSwitch, U64.of(0));
		}
		catch(Exception e) {
			log.info("eccezione in getSwitches: {}", e);
		}

		// avoid duplicates
	    List<NodePortTuple> switchList = route.getPath();
	    for (NodePortTuple npt: switchList){
	    	if(!switches.contains(npt.getNodeId())) // the pid is not already in the list
	    		switches.add(npt.getNodeId());
	    }
	    
		return switches;
	}

	private void deleteRules(String phy_address) {
		IPredicate[] predicates = {	new OperatorPredicate(COLUMN_R_SRC, Operator.EQ, phy_address),
									new OperatorPredicate(COLUMN_R_DST, Operator.EQ, phy_address)};
		CompoundPredicate predicate = new CompoundPredicate(CompoundPredicate.Operator.OR, false, predicates);
		IResultSet resultSet = storageSourceService.executeQuery(TABLE_RULES, new String[] {COLUMN_R_NAME}, predicate, null);
		Map<String, Object> row;

		// delete all flows with phy_address as source or destination address 
		for (Iterator<IResultSet> it = resultSet.iterator(); it.hasNext(); ) {
			row = it.next().getRow();
			String rule = (String)row.get(COLUMN_R_NAME);
			staticFlowEntryPusherService.deleteFlow(rule);
			log.info("deleted rule {}", rule);
		}
	}
	
	private void updateRules(String user, String newAddress) {
		// fetch all the addresses belonging to the user
		List<String> poolAddresses = SDNUtils.getPoolAddresses(storageSourceService, user);
		
		// for each old address, add a rule to connect to the new one
		for(String srcAddress : poolAddresses) {
			// skip the address itself
			if(srcAddress.equals(newAddress))
				continue;
			// get switches' PIDs that connect the two hosts
			List<DatapathId> switchesList = getSwitches(srcAddress, newAddress);
			//define new rule for each switch
			for(DatapathId sw : switchesList) {
				//each rule name must be unique, call them as srcAddress-dstAddress-switchPID
				String ruleName = srcAddress + "-" + newAddress + "-" + sw.toString();
				SDNUtils.addToRulesTable(storageSourceService, ruleName, srcAddress, newAddress);
				log.info("added rule {}", ruleName);

				// add new rule to the switch
				addNewFlowMod(ruleName, srcAddress, newAddress, sw, true);
			}
		}
		
		// add rules from the new one to all the old addresses
		for(String dstAddress : poolAddresses) {
			// skip the address itself
			if(dstAddress.equals(newAddress))
				continue;
			// get switches' PIDs that connect the two hosts
			List<DatapathId> switchesList = getSwitches(newAddress, dstAddress);
			//define new rule for each switch
			for(DatapathId sw : switchesList) {
				//each rule name must be unique, called them as srcAddress-dstAddress-switchPID
				String ruleName = newAddress + "-" + dstAddress + "-" + sw.toString();
				SDNUtils.addToRulesTable(storageSourceService, ruleName, newAddress, dstAddress);
				log.info("added rule {}", ruleName);

				// add new rule to the switch
				addNewFlowMod(ruleName, newAddress, dstAddress, sw, true);
			}
		}
	}
	
	/**
	 * adds new flowMod (rule) to the specified switch
	 * the rules added affect both ARP and IPv4 packets
	 * @param ruleName is the name of the rule to add
	 * @param srcAddress is the source address
	 * @param dstAddress is the destination address
	 * @param pid is the pid of the switch to add the rule on
	 * @param ARP set to true if you want to add the rule also for ARP packets
	 * */
	private void addNewFlowMod(String ruleName, String srcAddress, String dstAddress, DatapathId pid, boolean ARP) {
		// we assumed to use OF13
		// TODO make more dynamic by getting the version from the switch
		// (switchService.getSwitch(pid)).getOFFactory();
		OFFactory myFactory = OFFactoryVer13.INSTANCE;
    	OFFlowMod.Builder fmb = null;
    	OFActions actions = myFactory.actions();
    	ArrayList<OFAction> actionList = new ArrayList<OFAction>();
    	OFActionOutput output = actions.buildOutput()
    			.setPort(OFPort.ALL)
    			// TODO specify correct port
    			.build();
    	
    	actionList.add(output);
    	
    	Match myMatchARP = myFactory.buildMatch()
     			.setExact(MatchField.ETH_TYPE, EthType.ARP)
     			.setExact(MatchField.ARP_SPA, IPv4Address.of(srcAddress))
    			.setExact(MatchField.ARP_TPA, IPv4Address.of(dstAddress))
    			.build();
    	
    	Match myMatchIPv4 = myFactory.buildMatch()
    			.setExact(MatchField.ETH_TYPE,  EthType.IPv4)
    			.setExact(MatchField.IPV4_SRC, IPv4Address.of(srcAddress))
    			.setExact(MatchField.IPV4_DST, IPv4Address.of(dstAddress))
    			.build();
    	
    	fmb = myFactory.buildFlowAdd();
    	
    	OFFlowMod flowModARP = fmb.setActions(actionList)
				.setPriority(FlowModUtils.PRIORITY_MAX)
				.setMatch(myMatchARP)
				.build();
    	
    	OFFlowMod flowModIPv4 = fmb.setActions(actionList)
				.setPriority(FlowModUtils.PRIORITY_MAX)
				.setMatch(myMatchIPv4)
				.build();

    	staticFlowEntryPusherService.addFlow(ruleName, flowModIPv4, pid);
    	
    	if(ARP) {
    		ruleName += "-ARP";
			SDNUtils.addToRulesTable(storageSourceService, ruleName, srcAddress, dstAddress);
			log.info("added rule {}", ruleName);
    		staticFlowEntryPusherService.addFlow(ruleName, flowModARP, pid);

    	}
	}
	//****************************************************
	
	@Override
	public void rowsModified(String tableName, Set<Object> rowKeys){
		// called when a row of the table has been inserted or modified	
		log.info("row inserted in table {} - " + "ID:" + rowKeys.toString(), tableName);

		// update rules if the modified table is TABLE_SERVERS
		if(tableName == TABLE_SERVERS) {
			
			String user = null;
			String address = null;
			
			for (Object key : rowKeys) {
				IResultSet resultSet = storageSourceService.getRow(tableName, key);
				Iterator<IResultSet> it = resultSet.iterator();
				while (it.hasNext()) {
					Map<String, Object> row = it.next().getRow();
					user = (String) row.get(COLUMN_S_USER);
					address = (String) row.get(COLUMN_S_PHYSICAL);
					// row has been deleted
					if(user == null) {
						deleteRules(address);
					}
					// row has been added
					else {
						updateRules(user, address);
					}
				}
			}
		}
			
	}
	
	@Override
	public void rowsDeleted(String tableName, Set<Object> rowKeys){
		// called when a row of the table has been deleted
		log.info("row deleted from table {} - " + "ID:" + rowKeys.toString(), tableName);
	}
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return SDNProject.class.getSimpleName();
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		// modules that need to be placed before SDNProject
		return (type.equals(OFType.PACKET_IN) && (name.equals("devicemanager") || name.equals("topology")));
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) { 
		// modules that need to be placed after SDNProject
		return (type.equals(OFType.PACKET_IN) && (name.equals("forwarding")));
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(IRestApiService.class);
		l.add(IStorageSourceService.class);
		l.add(IStaticFlowEntryPusherService.class);
		
		l.add(IRoutingService.class);
		l.add(IDeviceService.class);
		l.add(ITopologyService.class);
		l.add(IOFSwitchService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProviderService = context.getServiceImpl(IFloodlightProviderService.class);
		restAPIService = context.getServiceImpl(IRestApiService.class);
		storageSourceService = context.getServiceImpl(IStorageSourceService.class);
		staticFlowEntryPusherService = context.getServiceImpl(IStaticFlowEntryPusherService.class);
		
		routingService = context.getServiceImpl(IRoutingService.class);
		deviceService = context.getServiceImpl(IDeviceService.class);
		switchService = context.getServiceImpl(IOFSwitchService.class);
		topologyService = context.getServiceImpl(ITopologyService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		restAPIService.addRestletRoutable(new SDNProjectRoutable());
		floodlightProviderService.addOFMessageListener(OFType.PACKET_IN, this);
		
		/* initialize network parameters */
		available_servers = tot_servers;
		
		log.info("TOT SERVERS: " + tot_servers);
		
		/* create tables */
				
		/* CREATE USERS TABLE */
		storageSourceService.createTable(TABLE_USERS, null);
		// column name is primary key
		storageSourceService.setTablePrimaryKeyName(TABLE_USERS, COLUMN_U_NAME);
		storageSourceService.addListener(TABLE_USERS, this);
		
		if(log.isDebugEnabled())
			log.debug("created table " + TABLE_USERS + ", with primary key " + COLUMN_U_NAME);

		/* CREATE RULES TABLE */
		storageSourceService.createTable(TABLE_RULES, null);
		// column rule_name is primary key
		storageSourceService.setTablePrimaryKeyName(TABLE_RULES, COLUMN_R_NAME);
		//storageSourceService.addListener(TABLE_RULES, this);

		if(log.isDebugEnabled())
			log.debug("created table " + TABLE_RULES + ", with primary key " + COLUMN_R_NAME);

		/* CREATE SERVERS TABLE */
		// column user is to be indexed
		Set<String> indexedColumns = new HashSet<String>();
		indexedColumns.add(COLUMN_S_USER);				
		storageSourceService.createTable(TABLE_SERVERS, indexedColumns);
		// column id is primary key
		storageSourceService.setTablePrimaryKeyName(TABLE_SERVERS, COLUMN_S_ID);
		storageSourceService.addListener(TABLE_SERVERS, this);
		
		if(log.isDebugEnabled())
			log.debug("created table " + TABLE_SERVERS + ", with primary key " + COLUMN_S_ID);

		//add entries to the servers table (all the server physical addresses and IDs)
		initServersTable(tot_servers);
}

	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
 
		// this should only return Command.STOP in order to drop all packets received by the controller
        
		switch (msg.getType()) {

		    case PACKET_IN:		    	
				return Command.STOP;
		    default:
				return Command.CONTINUE;
		    }
	}
	
	/**
	 * initializes the servers table with ID and Physical IP address
	 * ID is an incremental number, starting from 1
	 * IP address is assigned in an incremental manner starting from 10.0.0.1
	 * all other fields are filled with null values
	 * @param servers total number of servers
	 * */
	public void initServersTable(int servers){
		Integer ID = 1;
		for (int i=1; i<= servers; i++) {
			//skip addresses ending with 0
			if (i%256 == 0) {
				servers++; 
				continue;
			}
			// prepare row
			Map<String,Object> row = new HashMap<String,Object>();			
			row.put(COLUMN_S_ID, ID);
			row.put(COLUMN_S_PHYSICAL, FIRST_PHYSICAL_ADDR + i/256 +"." + (i%256));
			row.put(COLUMN_S_USER, null);
			row.put(COLUMN_S_VIRTUAL, null);
			
			storageSourceService.insertRow(TABLE_SERVERS, row);
			ID++;
			
			if (log.isDebugEnabled())
				log.info("new server added to table: " + row.toString()); //print the inserted server attributes
		}
			
	}
}
