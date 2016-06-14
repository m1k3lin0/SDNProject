/*
 * main module 
 */
package net.floodlightcontroller.sdnproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.OFFlowMod;
import org.projectfloodlight.openflow.protocol.OFFlowMod.Builder;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFlowModFlags;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionEnqueue;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.action.OFActionSetDlDst;
import org.projectfloodlight.openflow.protocol.action.OFActionSetField;
import org.projectfloodlight.openflow.protocol.action.OFActionSetNwDst;
import org.projectfloodlight.openflow.protocol.action.OFActions;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.oxm.OFOxms;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;
import org.projectfloodlight.openflow.types.TransportPort;
import org.projectfloodlight.openflow.types.VlanVid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.packet.UDP;
import net.floodlightcontroller.restserver.IRestApiService;
import net.floodlightcontroller.routing.ForwardingBase;
import net.floodlightcontroller.routing.IRoutingDecision;
import net.floodlightcontroller.routing.RoutingDecision;
import net.floodlightcontroller.sdnproject.web.SDNProjectRoutable;
import net.floodlightcontroller.staticflowentry.IStaticFlowEntryPusherService;
import net.floodlightcontroller.storage.IStorageSourceListener;
import net.floodlightcontroller.storage.IStorageSourceService;
import net.floodlightcontroller.util.FlowModUtils;
import net.floodlightcontroller.util.OFMessageDamper;

public class SDNProject implements IOFMessageListener, IFloodlightModule, IStorageSourceListener {

	protected static Logger log = LoggerFactory.getLogger(SDNProject.class);

	/* network parameters */
	//count the available servers, make private and implement method get and update?
	//get total number from python script and initialize in init
	protected static final int tot_servers = 10;
	public static int available_servers;
	
	public static final String FIRST_VIRTUAL_ADDR = "192.168.";
	public static final String FIRST_PHYSICAL_ADDR = "10.0.";

	/* module constant */
	
	// table of servers
	public static final String TABLE_SERVERS 		= "SDNProject_servers";
	public static final String COLUMN_S_ID			= "ID";
	public static final String COLUMN_S_PHYSICAL 	= "physical_address";
	public static final String COLUMN_S_VIRTUAL 	= "virtual_address";
	public static final String COLUMN_S_USER 		= "user";
	
	// table of the users
	public static final String TABLE_USERS		= "SDNProject_users";
	public static final String COLUMN_U_NAME 	= "user";
	public static final String COLUMN_U_SERVERS = "servers";
	
	
	protected IRestApiService restAPIService; //rest api
	protected IStorageSourceService storageSourceService; //to store the tables
	protected IFloodlightProviderService floodlightProviderService; //provider
	protected IStaticFlowEntryPusherService staticFlowEntryPusherService; //to handle the flow tables
	
	protected OFMessageDamper messageDamper;
	
	@Override
	public void rowsModified(String tableName, Set<Object> rowKeys){
		//called when a row of the table has been inserted or modified	
		log.info(": row inserted in table {} - " + "ID:" + rowKeys.toString(), tableName);
		
	
			
	}
	
	@Override
	public void rowsDeleted(String tableName, Set<Object> rowKeys){
		//called when a row of the table has been updated
		log.info(": row deleted from table {} - " + "ID:" + rowKeys.toString(), tableName);
	}
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return SDNProject.class.getSimpleName();
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		// TODO Auto-generated method stub
		return (type.equals(OFType.PACKET_IN) && (name.equals("devicemanager") || name.equals("topology"))); //after the devicemanager module
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// TODO Auto-generated method stub
		return (type.equals(OFType.PACKET_IN) && (name.equals("forwarding"))); //before of the forwarding module
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
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProviderService = context.getServiceImpl(IFloodlightProviderService.class);
		restAPIService = context.getServiceImpl(IRestApiService.class);
		storageSourceService = context.getServiceImpl(IStorageSourceService.class);
		staticFlowEntryPusherService = context.getServiceImpl(IStaticFlowEntryPusherService.class);
		
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
		// TODO Auto-generated method stub
		switch (msg.getType()) {

		    case PACKET_IN:

		        /* Retrieve the deserialized packet in message */
		        Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
		    			 
		        /* Various getters and setters are exposed in Ethernet */
		        MacAddress srcMac = eth.getSourceMACAddress();
		        VlanVid vlanId = VlanVid.ofVlan(eth.getVlanID());
		        
		        /* 
		         * Check the ethertype of the Ethernet frame and retrieve the appropriate payload.
		         * Note the shallow equality check. EthType caches and reuses instances for valid types.
		         */
		       if (eth.getEtherType() == EthType.IPv4) {
		            /* We got an IPv4 packet; get the payload from Ethernet */
		           
		        	//log.info("PACCHETTI IPV4");
		        	IPv4 ipv4 = (IPv4) eth.getPayload();
		        	//log.info("Source: "+ ipv4.getSourceAddress());
		        	//log.info("Destination: "+ ipv4.getDestinationAddress());
		        	 
		            /* Various getters and setters are exposed in IPv4 */
		            byte[] ipOptions = ipv4.getOptions();
		            IPv4Address dstIp = ipv4.getDestinationAddress();
		             
		            /* 
		             * Check the IP protocol version of the IPv4 packet's payload.
		             * Note the deep equality check. Unlike EthType, IpProtocol does
		             * not cache valid/common types; thus, all instances are unique.
		             */
		            if (ipv4.getProtocol().equals(IpProtocol.TCP)) {
		                /* We got a TCP packet; get the payload from IPv4 */
		                TCP tcp = (TCP) ipv4.getPayload();
		                
		                /* Various getters and setters are exposed in TCP */
		                TransportPort srcPort = tcp.getSourcePort();
		              //  log.info("*******SOURCE PORT: ", srcPort);
		                TransportPort dstPort = tcp.getDestinationPort();
		                short flags = tcp.getFlags();
		                 
		                /* Your logic here! */
		            } else if (ipv4.getProtocol().equals(IpProtocol.UDP)) {
		                /* We got a UDP packet; get the payload from IPv4 */
		                UDP udp = (UDP) ipv4.getPayload();
		  
		                /* Various getters and setters are exposed in UDP */
		                TransportPort srcPort = udp.getSourcePort();
		                //log.info("*******SOURCE PORT: "+ srcPort);
		                TransportPort dstPort = udp.getDestinationPort();
		                 
		                /* Your logic here! */
		        		return Command.CONTINUE;
		            }
		 
		        } else if (eth.getEtherType() == EthType.ARP) {
		        	log.info("PACCHETTI ARP");
		        	/* We got an ARP packet; get the payload from Ethernet */
		            ARP arp = (ARP) eth.getPayload();
		            log.info("Lo switch e : "+ sw.getId().toString());
		            log.info("Source: "+ arp.getSenderProtocolAddress());
		            log.info("Source: "+ eth.getSourceMACAddress());
		        	log.info("Destination: "+ arp.getTargetProtocolAddress());
		        	log.info("Destination: "+ eth.getDestinationMACAddress());
		        	
		        	
		        	
		        	//PROVA AD AGGIUNGERE UNA REGOLA
		        	OFFactory myFactory = sw.getOFFactory();
		        	OFFlowMod.Builder fmb = null;
		        	OFActions actions = myFactory.actions();
		        	ArrayList<OFAction> actionList = new ArrayList<OFAction>();
		        	OFActionOutput output = actions.buildOutput().setPort(OFPort.ALL).build();
		        	
		        	
		        	
		        	actionList.add(output);
		        	Match myMatch = myFactory.buildMatch()
		        			//.setExact(MatchField.IN_PORT, OFPort.of(1))
		         			.setExact(MatchField.ETH_TYPE, EthType.ARP)
		         			.setExact(MatchField.ARP_SPA, IPv4Address.of("10.0.0.1"))
		        			.setExact(MatchField.ARP_TPA, IPv4Address.of("10.0.0.2"))
		        			.build();
		        	
		        	Match myMatch3 = myFactory.buildMatch()
		        			.setExact(MatchField.ETH_TYPE,  EthType.IPv4)
		        			.setExact(MatchField.IPV4_SRC, IPv4Address.of("10.0.0.1"))
		        			.setExact(MatchField.IPV4_DST, IPv4Address.of("10.0.0.2"))
		        			.build();
		        	
		        	fmb = OFFactories.getFactory(sw.getOFFactory().getVersion()).buildFlowAdd(); //modify o add ?
		        	
		        	OFFlowMod flowMod = fmb.setActions(actionList)
    						.setPriority(FlowModUtils.PRIORITY_MAX)
    						.setMatch(myMatch)
    						.build();
		        	OFFlowMod flowMod3 = fmb.setActions(actionList)
    						.setPriority(FlowModUtils.PRIORITY_MAX)
    						.setMatch(myMatch3)
    						.build();
    	
		        	//staticFlowEntryPusherService.addFlow("regola1", flowMod, DatapathId.of("00:00:00:00:00:00:00:02"));
		        	staticFlowEntryPusherService.addFlow("regola1010", flowMod3, DatapathId.of("00:00:00:00:00:00:00:02"));
		        	//staticFlowEntryPusherService.addFlow("regola2", flowMod, DatapathId.of("00:00:00:00:00:00:00:01"));
		        	//staticFlowEntryPusherService.addFlow("regola3", flowMod, DatapathId.of("00:00:00:00:00:00:00:03"));
		        	
		        	
		     
		        	//seconda regola
		        	OFFlowMod.Builder fmb1 = null;
		        	Match myMatch2 = myFactory.buildMatch()
		        			//.setExact(MatchField.IN_PORT, OFPort.of(2))
		         			.setExact(MatchField.ETH_TYPE, EthType.ARP)
		         			.setExact(MatchField.ARP_SPA, IPv4Address.of("10.0.0.2"))
		        			.setExact(MatchField.ARP_TPA, IPv4Address.of("10.0.0.1"))
		        			.build();
		        	
		        	Match myMatch4 = myFactory.buildMatch()
		        			.setExact(MatchField.ETH_TYPE,  EthType.IPv4)
		        			.setExact(MatchField.IPV4_SRC, IPv4Address.of("10.0.0.2"))
		        			.setExact(MatchField.IPV4_DST, IPv4Address.of("10.0.0.1"))
		        			.build();
		        	
		        	ArrayList<OFAction> actionList1 = new ArrayList<OFAction>();
		        	OFActionOutput output1 = actions.buildOutput().setPort(OFPort.ALL).build();
		        	actionList1.add(output1);

		        	
		        	
		        	fmb1 = OFFactories.getFactory(sw.getOFFactory().getVersion()).buildFlowAdd(); //modify o add ?
		        	
		        	
		        	
		        	OFFlowMod flowMod1 = fmb1.setActions(actionList1)
    						.setPriority(FlowModUtils.PRIORITY_MAX)
    						.setMatch(myMatch2)
    						.build();
		
		        	OFFlowMod flowMod4 = fmb1.setActions(actionList1)
    						.setPriority(FlowModUtils.PRIORITY_MAX)
    						.setMatch(myMatch4)
    						.build();
    	
		        	//staticFlowEntryPusherService.addFlow("regola4", flowMod1, DatapathId.of("00:00:00:00:00:00:00:02"));
		        	staticFlowEntryPusherService.addFlow("regola4040", flowMod4, DatapathId.of("00:00:00:00:00:00:00:02"));
		        	//staticFlowEntryPusherService.addFlow("regola5", flowMod1, DatapathId.of("00:00:00:00:00:00:00:01"));
		        	//staticFlowEntryPusherService.addFlow("regola6", flowMod1, DatapathId.of("00:00:00:00:00:00:00:03"));
		 
		            /* Various getters and setters are exposed in ARP */
		            boolean gratuitous = arp.isGratuitous();
		            
		            return Command.CONTINUE;
		 
		        } else {
		            /* Unhandled ethertype */
		        }
		        break;
		    default:
		        break;
		    }
		
			//RoutingDecision.rtStore.put(cntx, key, IRoutingDecision.CONTEXT_DECISION)
		
			return Command.STOP;
		
	}
	
	/**
	 * initializes the servers table with ID and Physical IP address
	 * ID is an incremental number, starting from 1
	 * IP address is assigned in an incremental manner starting from 10.0.0.1
	 * all other fields are filled with null value
	 * @param servers : total number of servers
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
				log.info("new server added in table: " + row.toString()); //print the inserted server attributes
		}
			
	}
}
