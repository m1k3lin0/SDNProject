/*
 * main module 
 */
package net.floodlightcontroller.sdnproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.restserver.IRestApiService;
import net.floodlightcontroller.storage.IStorageSourceListener;
import net.floodlightcontroller.storage.IStorageSourceService;

public class SDNProject implements IOFMessageListener, IFloodlightModule, IStorageSourceListener {

	protected static Logger log = LoggerFactory.getLogger(SDNProject.class);

	//count the available servers, make private and implement method get and update?
	//get total number from python script
	protected static int available_servers = 1000;
	
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
	
	@Override
	public void rowsModified(String tableName, Set<Object> rowKeys){
		//called when a row of the table has been inserted or modified	
		log.info(": user inserted in table {}: " + rowKeys.toString(), tableName);
	}
	
	@Override
	public void rowsDeleted(String tableName, Set<Object> rowKeys){
		//called when a row of the table has been updated
	}
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
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
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		restAPIService.addRestletRoutable(new SDNProjectRoutable());
		//floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
		
		/* create tables */
		
		// create users table
		storageSourceService.createTable(TABLE_USERS, null);
		// column name is primary key
		storageSourceService.setTablePrimaryKeyName(TABLE_USERS, COLUMN_U_NAME);
		storageSourceService.addListener(TABLE_USERS, this);
		
		if(log.isDebugEnabled())
			log.debug("created table " + TABLE_USERS + ", with primary key " + COLUMN_U_NAME);


		log.info("TABLES CREATED: " + storageSourceService.getAllTableNames());
				
		// column user is to be indexed
		Set<String> indexedColumns = new HashSet<String>();
		indexedColumns.add(COLUMN_S_USER);
		// create servers table
		storageSourceService.createTable(TABLE_SERVERS, indexedColumns);
		// column id is primary key
		storageSourceService.setTablePrimaryKeyName(TABLE_SERVERS, COLUMN_S_ID);
		storageSourceService.addListener(TABLE_SERVERS, this);
		
		//TODO add entries to the servers table (all the server physical addresses and IDs)
	}

	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * Parses a JSON data into a Map
	 * Exploits jackson
	 * Expects a string in JSON that follows the following syntax:
	 *        {
	 *            "user"	: "name",
	 *            "servers"	: 10,
	 *        }
	 * Ignores all other specified fields, if any
	 * @param jsonData The JSON formatted data
	 * @return The map of the storage entry
	 * @throws IOException If there was an error parsing the JSON
	 */
	public static Map<String, Object> jParse(String jsonData) throws IOException {
		Map<String, Object> entry = new HashMap<String, Object>();
		MappingJsonFactory f = new MappingJsonFactory();
		JsonParser parser;

		try {
			parser = f.createParser(jsonData);
		} catch (JsonParseException e) {
			throw new IOException(e);
		}

		// first call to next should return START_OBJECT
		parser.nextToken();
		if (parser.getCurrentToken() != JsonToken.START_OBJECT) {
			throw new IOException("Expected START_OBJECT");
		}

		// last call to next should return END_OBJECT
		while (parser.nextToken() != JsonToken.END_OBJECT) {
			if (parser.getCurrentToken() != JsonToken.FIELD_NAME) {
				throw new IOException("Expected FIELD_NAME");
			}

			String current = parser.getCurrentName();
			parser.nextToken();

			/* name of json fields follow the defined column names */
			switch (current) {
			case SDNProject.COLUMN_U_NAME:
				entry.put(SDNProject.COLUMN_U_NAME, parser.getText());
				break;
			case SDNProject.COLUMN_U_SERVERS:
				//check if numeric or "all"
				//if(parser.getCurrentToken()==JsonToken.VALUE_STRING)
				//getText() works for any token type
				entry.put(SDNProject.COLUMN_U_SERVERS, parser.getText());
				break;
			default:
				log.error("Could not decode field from JSON string: {}", current);
				break;
			}
			
			if(log.isDebugEnabled()) {
				log.debug("parsing field {}, with value {}", current, parser.getText());
			}
		}
		
		return entry;
	}

}
