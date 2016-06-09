/*
 * main module 
 */
package net.floodlightcontroller.sdnproject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.projectfloodlight.openflow.protocol.OFFlowMod;
import org.projectfloodlight.openflow.protocol.OFFlowMod.Builder;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFlowModFlags;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
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
import net.floodlightcontroller.staticflowentry.IStaticFlowEntryPusherService;
import net.floodlightcontroller.storage.IStorageSourceListener;
import net.floodlightcontroller.storage.IStorageSourceService;

public class SDNProject implements IOFMessageListener, IFloodlightModule, IStorageSourceListener {

	protected static Logger log = LoggerFactory.getLogger(SDNProject.class);

	/* network parameters */
	//count the available servers, make private and implement method get and update?
	//get total number from python script and initialize in init
	protected static final int tot_servers = 10;
	protected static int available_servers;
	
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
	
	
	@Override
	public void rowsModified(String tableName, Set<Object> rowKeys){
		//called when a row of the table has been inserted or modified	
		log.info(": row inserted in table {} - " + "ID:" + rowKeys.toString(), tableName);
		
	/*	//PROVA ADDFLOW
		
		if(tableName == TABLE_USERS){
			OFFlowMod.Builder fm = null;
			fm = OFFactories.getFactory(OFVersion.OF_13).buildFlowModify();
			fm.setIdleTimeout(0);
			fm.setHardTimeout(0);
			fm.setBufferId(OFBufferId.NO_BUFFER);
			fm.setOutPort(OFPort.ANY);
			fm.setPriority(Integer.MAX_VALUE);
			fm.setFlags(Collections.singleton(OFFlowModFlags.SEND_FLOW_REM));
	
			//DatapathId swDpid = null;
			String swDpid = "00:00:00:00:00:00:00:01";
		
			DatapathId ciccio = DatapathId.of(swDpid);
			staticFlowEntryPusherService.addFlow("flow1", (OFFlowMod) fm, ciccio);

			log.info("sono riuscito a modificare le cose");
			//
		}*/
			
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
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// TODO Auto-generated method stub
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
		// log.info("NON SO CHE FARE CON QUESTO PACCHETTO");		
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
		           
		        	log.info("sono di tipo IPV4");
		        	IPv4 ipv4 = (IPv4) eth.getPayload();
		             
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
		                log.info("*******SOURCE PORT: ", srcPort);
		                TransportPort dstPort = tcp.getDestinationPort();
		                short flags = tcp.getFlags();
		                 
		                /* Your logic here! */
		            } else if (ipv4.getProtocol().equals(IpProtocol.UDP)) {
		                /* We got a UDP packet; get the payload from IPv4 */
		                UDP udp = (UDP) ipv4.getPayload();
		  
		                /* Various getters and setters are exposed in UDP */
		                TransportPort srcPort = udp.getSourcePort();
		                log.info("*******SOURCE PORT: ", srcPort);
		                TransportPort dstPort = udp.getDestinationPort();
		                 
		                /* Your logic here! */
		            }
		 
		        } else if (eth.getEtherType() == EthType.ARP) {
		        	log.info("sono di tipo ARP");
		            /* We got an ARP packet; get the payload from Ethernet */
		            ARP arp = (ARP) eth.getPayload();
		 
		            /* Various getters and setters are exposed in ARP */
		            boolean gratuitous = arp.isGratuitous();
		 
		        } else {
		            /* Unhandled ethertype */
		        }
		        break;
		    default:
		        break;
		    }
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
