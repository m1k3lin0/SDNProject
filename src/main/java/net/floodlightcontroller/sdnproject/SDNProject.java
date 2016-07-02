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
import org.projectfloodlight.openflow.protocol.action.OFActionSetField;
import org.projectfloodlight.openflow.protocol.action.OFActions;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.oxm.OFOxms;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.U64;
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
import net.floodlightcontroller.restserver.IRestApiService;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.sdnproject.web.SDNProjectRoutable;
import net.floodlightcontroller.staticflowentry.IStaticFlowEntryPusherService;
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

public class SDNProject implements IOFMessageListener, IFloodlightModule, IStorageSourceListener {

	protected static Logger log = LoggerFactory.getLogger(SDNProject.class);

	/* network parameters
	 * parameters are initialized in init() method, according to config file floodlightdeafault.properties 
	 * the network topology follows the tree topology of mininet, defined by two parameters
	 * 	FANOUT: is the number of hosts/switch connected to the parent
	 * 	DEPTH : is the level of the tree
	 * further parameters are
	 * 	FIRST_PUBLIC_ADDR : is the base from where to start to assign the public ip address to provide the client
	 * 	FIRST_PRIVATE_ADDR: is the ip address from which mininet starts its sequential assignment
	 * 	BROADCAST_ADDR	  : is the broadcast address in the private network
	 * */
	protected static int FANOUT;
	protected static int DEPTH;
	protected static int tot_servers;
	public static int available_servers;

	public static String DOMAIN;
	public static String FIRST_PRIVATE_ADDR;
	public static String BROADCAST_ADDR;

	/* module constants */
	// table of servers
	public static final String TABLE_SERVERS 	= "SDNProject_servers";
	public static final String COLUMN_S_ID		= "ID";
	public static final String COLUMN_S_PRIVATE	= "private_address";
	public static final String COLUMN_S_PUBLIC 	= "public_address";
	public static final String COLUMN_S_USER 	= "user";
	public static final String COLUMN_S_OLDUSER = "old_user";
	
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
	protected ITopologyService topologyService;
	protected IOFSwitchService switchService;

	/* module private methods */
	/**
	 * initializes the servers table with ID and private IP address
	 * ID is an incremental number, starting from 1
	 * IP address is assigned in an incremental manner starting from 10.0.0.1
	 * all other fields are filled with null values
	 * @param servers total number of servers
	 * */
	private void initServersTable() {
		Integer ID = 1;
		int servers = tot_servers;
		for (int i=1; i<= servers; i++) {
			//skip addresses ending with 0
			if (i%256 == 0) {
				servers++; 
				continue;
			}
			// prepare row
			Map<String,Object> row = new HashMap<String,Object>();			
			row.put(COLUMN_S_ID, ID);
			row.put(COLUMN_S_PRIVATE, FIRST_PRIVATE_ADDR + i/256 +"." + (i%256));
			row.put(COLUMN_S_USER, null);
			row.put(COLUMN_S_PUBLIC, null);
			
			storageSourceService.insertRow(TABLE_SERVERS, row);
			ID++;
		}			
	}
	
	/**
	 * finds the edge switch the specified IP address is attached to
	 * @param IPAddress is the IP address of the host
	 * @return the pair of pid and port of the attachment point of the specified IP
	 * */
	private NodePortTuple getAP(String IPAddress) {
		// TODO modify this method if we want to get the AP independently from the topology 
		Set<DatapathId> allSwitches = switchService.getAllSwitchDpids(); //get all the switches in the topology
		List<DatapathId> edgeSwitches = new ArrayList<DatapathId>();
		//get all the edge switches in the topology
		for (DatapathId sw : allSwitches) {
			if(topologyService.isEdge(sw, OFPort.of(1))){ //it is sufficient to check only the first port
				edgeSwitches.add(sw);
			}
		}
		Collections.sort(edgeSwitches); //sort
		
		int range = IPv4Address.of(IPAddress).getInt()-IPv4Address.of(FIRST_PRIVATE_ADDR+"0.1").getInt();
		int index = range/FANOUT;
		
		int port = (range%FANOUT) + 1;
		
		return new NodePortTuple(edgeSwitches.get(index),OFPort.of(port));
	}
	
	/**
	 * finds all the switches on the path that connect two devices identified by
	 * the provided IP addresses
	 * @param sourceAddress is the IP address of the source host
	 * @param destAddress is the IP address of the destination host
	 * @return a list of all the switches on the path and relative outport as List of NodePortTuple
	 * */
	private List<NodePortTuple> getSwitchesInPath(String sourceAddress, String destAddress) {
		
		List<NodePortTuple> switchList = new ArrayList<NodePortTuple>(); //list for the switches in the path
		List<NodePortTuple> switches = new ArrayList<NodePortTuple>(); //list for the switches+outport
		//compute the first switch+port connected to ip address
		NodePortTuple srcNode = getAP(sourceAddress);
		NodePortTuple dstNode = getAP(destAddress);
		//compute the 2 switches
		DatapathId srcSwitch = srcNode.getNodeId();
		DatapathId dstSwitch = dstNode.getNodeId();
		
		//if they are equal, there's no need to compute the route because the switch is just one
		if(srcSwitch.equals(dstSwitch)){
			switches.add(dstNode);
			return switches;
		}
		
		// take the switches traversed by the path between source and destination
		Route route = null;
		
		try {
			route = routingService.getRoute(srcSwitch, dstSwitch, U64.of(0));
		}
		catch(Exception e) {
			log.error("Exception in getSwitches. {}", e);
		}

	    switchList = route.getPath();
	    
	    //route returns duplicated links, so need to prune it down
	    for (int i = 0; i<switchList.size(); i+=2){
	    	if(!switches.contains(switchList.get(i))){ //don't add duplicates
	    		if(!switchList.get(i).getNodeId().equals(dstNode.getNodeId())) //we add the edge switch manually because the route doesn't know the port the hosts are attached to
	    			switches.add(switchList.get(i));		    		
	    	}
	    }
	    switches.add(dstNode);
	    return switches;
	}

	
	/**
	 * removes all the rules relative to the specified address
	 * @param address is the address that has been removed from the pool
	 * */
	private void deleteRules(String address) {
		IPredicate[] predicates = {	new OperatorPredicate(COLUMN_R_SRC, Operator.EQ, address),
									new OperatorPredicate(COLUMN_R_DST, Operator.EQ, address)};
		CompoundPredicate predicate = new CompoundPredicate(CompoundPredicate.Operator.OR, false, predicates);
		IResultSet resultSet = storageSourceService.executeQuery(TABLE_RULES, new String[] {COLUMN_R_NAME}, predicate, null);
		Map<String, Object> row;

		// delete all flows with address as source or destination address 
		for (Iterator<IResultSet> it = resultSet.iterator(); it.hasNext(); ) {
			row = it.next().getRow();
			String rule = (String)row.get(COLUMN_R_NAME);
			storageSourceService.deleteRow(SDNProject.TABLE_RULES, rule);
			staticFlowEntryPusherService.deleteFlow(rule);
			log.info("Deleted rule {}", rule);
		}
	}
	
	/**
	 * updates all the rules relative to the specified address belonging to the pool of the specified user 
	 * @param user is the owner of the pool
	 * @param newAddress is the address that has been added to the pool
	 * */
	private void updateRules(String user, String newAddress) {
		// fetch all the addresses belonging to the user
		List<String> poolAddresses = SDNUtils.getPoolAddresses(storageSourceService, user);
		
		// for each old address, add a rule to connect to the new one
		for(String srcAddress : poolAddresses) {
			// skip the address itself
			if(srcAddress.equals(newAddress))
				continue;
			// get switches' PIDs that connect the two hosts
			List<NodePortTuple> switchesList = getSwitchesInPath(srcAddress, newAddress);
			//define new rule for each switch
			for(NodePortTuple sw : switchesList) {
				//each rule name must be unique, call them as srcAddress-dstAddress-switchPID
				String ruleName = srcAddress + "-" + newAddress + "-" + sw.toString();
				SDNUtils.addToRulesTable(storageSourceService, ruleName, srcAddress, newAddress);
				
				// add new rule to the switch
				addNewFlowMod(ruleName, srcAddress, newAddress, sw.getNodeId(), sw.getPortId(), true);
			}
		}
		
		// add rules from the new one to all the old addresses
		for(String dstAddress : poolAddresses) {
			// skip the address itself
			if(dstAddress.equals(newAddress))
				continue;
			// get switches' PIDs that connect the two hosts
			List<NodePortTuple> switchesList = getSwitchesInPath(newAddress, dstAddress);
			//define new rule for each switch
			for(NodePortTuple sw : switchesList) {
				//each rule name must be unique, called them as srcAddress-dstAddress-switchPID
				String ruleName = newAddress + "-" + dstAddress + "-" + sw.toString();
				SDNUtils.addToRulesTable(storageSourceService, ruleName, newAddress, dstAddress);
				
				// add new rule to the switch
				addNewFlowMod(ruleName, newAddress, dstAddress, sw.getNodeId(), sw.getPortId(), true);
			}
		}
	}
	
	
	/**
	 * adds to the action list 2 actions: one that substitutes the destination address with the specified ip
	 * the other that specifies on which port to output the packets
	 * @param edgeSwitch is the pair port-pid of the switch the action needs to be set on
	 * @param destIP is the new destination address
	 * @param actionList is the action list to which the actions need to be added
	 * */
	private void addAction(NodePortTuple edgeSwitch, String destIP, List<OFAction> actionList){
		OFFactory myFactory = switchService.getSwitch(edgeSwitch.getNodeId()).getOFFactory();
		OFOxms oxms = myFactory.oxms();
    	OFActions actions = myFactory.actions();
    	
    	OFActionSetField newDest = actions.buildSetField()
    		    .setField(
    		        oxms.buildIpv4Dst()
    		        .setValue(IPv4Address.of(destIP))
    		        .build()
    		    )
    		    .build();
    	
    	OFActionOutput output = actions.buildOutput()
    			.setPort(edgeSwitch.getPortId())
    			.build();
    	
    	actionList.add(newDest);
    	actionList.add(output);   	
	}
	
	/**
	 * adds all the broadcast rules relative to the pool of the specified user
	 * @param user is the owner of the pool
	 * */
	private void addBroadcastRules(String user){
		NodePortTuple edgeSwitch = null;
		List<String> poolAddresses = SDNUtils.getPoolAddresses(storageSourceService, user);
		if(poolAddresses.size()==1) 
			return;
		
    	for(String sourceIP : poolAddresses){
        	ArrayList<OFAction> actionList = new ArrayList<OFAction>();
	
    		for(String destIP : poolAddresses){
    			if(sourceIP.equals(destIP))
    				continue;
    			
    			edgeSwitch = getSwitchesInPath(sourceIP, destIP).get(0);
    			addAction(edgeSwitch, destIP, actionList);
    		}
    		
    		OFFactory myFactory = switchService.getSwitch(edgeSwitch.getNodeId()).getOFFactory();
    		OFFlowMod.Builder fmb = myFactory.buildFlowAdd();
    				
        	Match myMatchBroad = myFactory.buildMatch()
         			.setExact(MatchField.ETH_TYPE, EthType.IPv4)
         			.setExact(MatchField.IPV4_SRC, IPv4Address.of(sourceIP))
        			.setExact(MatchField.IPV4_DST, IPv4Address.of(BROADCAST_ADDR))
        			.build();
        	
        	OFFlowMod flowModBroadcast = fmb.setActions(actionList)
    				.setPriority(FlowModUtils.PRIORITY_MAX)
    				.setMatch(myMatchBroad)
    				.build();		
    		
        	String ruleName = sourceIP + "-" + BROADCAST_ADDR + "-BROADCAST";
        	
        	//add flow only if it isn't already present
        	//if(!SDNUtils.alreadyInTable(storageSourceService, TABLE_RULES, COLUMN_R_NAME, ruleName)){
            	SDNUtils.addToRulesTable(storageSourceService, ruleName, sourceIP, BROADCAST_ADDR);
        		staticFlowEntryPusherService.addFlow(ruleName, flowModBroadcast, edgeSwitch.getNodeId());
        		log.info("added rule {}", ruleName);	
        	//}

    	} 	
	}
	
	/**
	 * removes all the broadcast rules relative to the pool of the specified user
	 * @param user is the owner of the pool
	 * */
	private void removeBroadcastRules(String user) {
		List<String> poolAddresses = SDNUtils.getPoolAddresses(storageSourceService, user);
				
		for(String sourceAddress : poolAddresses) {
			IPredicate[] predicates = {	new OperatorPredicate(COLUMN_R_SRC, Operator.EQ, sourceAddress),
					new OperatorPredicate(COLUMN_R_DST, Operator.EQ, BROADCAST_ADDR)};
			CompoundPredicate predicate = new CompoundPredicate(CompoundPredicate.Operator.AND, false, predicates);
			IResultSet resultSet = storageSourceService.executeQuery(TABLE_RULES, new String[] {COLUMN_R_NAME}, predicate, null);
			Map<String, Object> row = resultSet.iterator().next().getRow();
			// there is only one rule for each source address with destination broadcast
			String rule = (String)row.get(COLUMN_R_NAME);
			storageSourceService.deleteRow(SDNProject.TABLE_RULES, rule);
			staticFlowEntryPusherService.deleteFlow(rule);
			log.info("Deleted rule {}", rule);
		}
	}
	
	/**
	 * adds new flowMod (rule) to the specified switch
	 * the rules added affect both ARP and IPv4 packets
	 * @param ruleName is the name of the rule to add
	 * @param srcAddress is the source address
	 * @param dstAddress is the destination address
	 * @param pid is the pid of the switch to add the rule on
	 * @param outPort is the port on which the output needs to be forwarded
	 * @param ARP set to true if you want to add the rule also for ARP packets
	 * */
	private void addNewFlowMod(String ruleName, String srcAddress, String dstAddress, DatapathId pid, OFPort outPort, boolean ARP) {
		OFFactory myFactory = switchService.getSwitch(pid).getOFFactory();
    	OFFlowMod.Builder fmb = null;
    	OFActions actions = myFactory.actions();
    	ArrayList<OFAction> actionList = new ArrayList<OFAction>();
    	OFActionOutput output = actions.buildOutput()
    			.setPort(outPort)
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
		log.info("added rule {}", ruleName);
				
    	if(ARP) {
    		ruleName += "-ARP";
			SDNUtils.addToRulesTable(storageSourceService, ruleName, srcAddress, dstAddress);
    		staticFlowEntryPusherService.addFlow(ruleName, flowModARP, pid);
			log.info("added rule {}", ruleName);

    	}
	}
	/* end of private methods */
	
	@Override
	public void rowsModified(String tableName, Set<Object> rowKeys){
		// called when a row of the table has been inserted or modified

		// update rules if the modified table is TABLE_SERVERS
		if(tableName == TABLE_SERVERS) {
			
			String user = null;
			String address = null;
			String removedUser = null;
			
			for (Object key : rowKeys) {
				IResultSet resultSet = storageSourceService.getRow(tableName, key);
				Iterator<IResultSet> it = resultSet.iterator();
				while (it.hasNext()) {
					Map<String, Object> row = it.next().getRow();
					user = (String) row.get(COLUMN_S_USER);
					address = (String) row.get(COLUMN_S_PRIVATE);
					removedUser = (String) row.get(COLUMN_S_OLDUSER);
					
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
			if(user != null) {
				// new hosts have been assigned
				addBroadcastRules(user);
			}
			else if (removedUser != null) {
				// hosts have been removed
				// TODO add broadcast rules just once
				removeBroadcastRules(removedUser); // remove all broadcast rules
				addBroadcastRules(removedUser); // add broadcast rules for hosts still in the pool
			}
		}		
	}
	
	@Override
	public void rowsDeleted(String tableName, Set<Object> rowKeys){
		// called when a row of the table has been deleted
	}
	
	@Override
	public String getName() {
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
		switchService = context.getServiceImpl(IOFSwitchService.class);
		topologyService = context.getServiceImpl(ITopologyService.class);

		// Initialize network parameters from config file
		Map<String, String> configParams = context.getConfigParams(this);
		FANOUT = Integer.parseInt(configParams.get("fanout"));
		DEPTH = Integer.parseInt(configParams.get("depth"));
		FIRST_PRIVATE_ADDR = configParams.get("private_ipbase");
		DOMAIN = configParams.get("domain");
		BROADCAST_ADDR = configParams.get("bcast_address");
		tot_servers = (int) Math.pow(FANOUT, DEPTH);
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		restAPIService.addRestletRoutable(new SDNProjectRoutable());
		floodlightProviderService.addOFMessageListener(OFType.PACKET_IN, this);
		
		/* initialize network parameters */
		available_servers = tot_servers;
		log.info("Total number of servers: " + tot_servers);
				
		/* create users table */
		storageSourceService.createTable(TABLE_USERS, null);
		// column name is primary key
		storageSourceService.setTablePrimaryKeyName(TABLE_USERS, COLUMN_U_NAME);
		storageSourceService.addListener(TABLE_USERS, this);
		log.info("Created table {}", TABLE_USERS);

		/* create rules table */
		storageSourceService.createTable(TABLE_RULES, null);
		// column rule_name is primary key
		storageSourceService.setTablePrimaryKeyName(TABLE_RULES, COLUMN_R_NAME);
		log.info("Created table {}", TABLE_RULES);

		/* create servers table */
		// column user is to be indexed
		Set<String> indexedColumns = new HashSet<String>();
		indexedColumns.add(COLUMN_S_USER);				
		storageSourceService.createTable(TABLE_SERVERS, indexedColumns);
		// column id is primary key
		storageSourceService.setTablePrimaryKeyName(TABLE_SERVERS, COLUMN_S_ID);
		storageSourceService.addListener(TABLE_SERVERS, this);
		log.info("Created table {}", TABLE_SERVERS);

		//add entries to the servers table (all the server private addresses and IDs)
		initServersTable();
}

	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {    
		switch (msg.getType()) {
		
		    case PACKET_IN:		
				// drop all packets PACKET_IN received by the controller    	
				return Command.STOP;
		    default:
				return Command.CONTINUE;
		    }
	}
	
}
