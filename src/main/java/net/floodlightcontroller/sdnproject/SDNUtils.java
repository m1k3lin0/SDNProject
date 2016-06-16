package net.floodlightcontroller.sdnproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;

import net.floodlightcontroller.storage.IResultSet;
import net.floodlightcontroller.storage.IStorageSourceService;
import net.floodlightcontroller.storage.OperatorPredicate;
import net.floodlightcontroller.storage.OperatorPredicate.Operator;

public final class SDNUtils {
	
	protected static Logger log = LoggerFactory.getLogger(SDNProject.class);
	
	/**
	 * Parses a JSON data into a Map
	 * Exploits jackson
	 * Expects a string in JSON that follows the following syntax:
	 *        {
	 *            "user"	: "name",
	 *            "servers"	: 10,
	 *        }
	 * Ignores all other specified fields, if any
	 * @param jsonData is The JSON formatted data
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
				if(parser.getCurrentToken()==JsonToken.VALUE_NUMBER_INT)
					//getText() works for any token type
					entry.put(SDNProject.COLUMN_U_SERVERS, parser.getText());
				else
					//set value as the maximum if "all"
					entry.put(SDNProject.COLUMN_U_SERVERS, SDNProject.tot_servers);
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

	/**
	 * checks if a user is already in the users table
	 * @param storageSource is reference to the storage source containing the table
	 * @param user is	name of the user
	 * @return true if the user already exists in the table
	 * */
	public static boolean userExists(IStorageSourceService storageSource, String user) {
		OperatorPredicate predicate = new OperatorPredicate(SDNProject.COLUMN_U_NAME, Operator.EQ, user);
		IResultSet resultSet = storageSource.executeQuery(SDNProject.TABLE_USERS, new String[] {SDNProject.COLUMN_U_NAME}, predicate, null);
		if(resultSet.iterator().hasNext())
			return true;
		return false;	
	}
	
	/**
	 * fetches the number of servers belonging to a user
	 * @param storageSource is reference to the storage source containing the table
	 * @param user is name of the user
	 * @return the number of servers that belongs to the user 
	 * */
	public static int getServers(IStorageSourceService storageSource, String user) {
		OperatorPredicate predicate = new OperatorPredicate(SDNProject.COLUMN_U_NAME, Operator.EQ, user);
		IResultSet resultSet = storageSource.executeQuery(SDNProject.TABLE_USERS, 
				new String[] {SDNProject.COLUMN_U_NAME, SDNProject.COLUMN_U_SERVERS}, predicate, null);
		Map<String, Object> row;
		int servers = -1;
		// COLUMN_U_NAME is primary key, hence only one result is possible
		try {
			row = resultSet.iterator().next().getRow();
			log.info("getServers: row: " + row.toString());
			servers = (int) row.get(SDNProject.COLUMN_U_SERVERS);
		}
		catch(NoSuchElementException e) {
			log.error("No result for user {}", user, e);
		}
		
		return servers;
	}
	
	/**
	 * assigns requested server to a specific user by updating the servers table
	 * assigns new virtual addresses, starting from 1 if it is a new request
	 * starting from the last one if it is an add request
	 * @param servers is the number of servers requested
	 * @param user is the user requiring the servers
	 * @param storageSource is reference to the storage source containing the table
	 * */
	public static void assignServers(IStorageSourceService storageSource, int servers, String user) {
		String virtualAddr = null;
		int previous = 0;
		// if user already existent, start from last assigned virtual address
		if(userExists(storageSource, user)) {
			previous = getServers(storageSource, user);
		}
		for(int i=previous+1; i<=(servers+previous); i++) {
			if(i%256 ==0 ) {
				servers++;
				continue;
			}
			Map<String,Object> row = new HashMap<String,Object>();
			Integer ID = SDNUtils.getFirstFreeServer(storageSource);
			log.info("assignServers: first free server ID: " + ID);
			virtualAddr = SDNProject.FIRST_VIRTUAL_ADDR + i/256 + "." + i%256; 
			row.put(SDNProject.COLUMN_S_USER, user);
			row.put(SDNProject.COLUMN_S_VIRTUAL, virtualAddr);
			storageSource.updateRow(SDNProject.TABLE_SERVERS, ID, row);
			log.info("assignServers: assigned server " + ID + " to user {}. VIR: {}", user, virtualAddr);
		}
	}
	
	/**
	 * remove a certain number of server from the specified client by resetting to null
	 * fields user and virtual_address relative to the entries with the highest virtual_addresses
	 * @param servers is the number of servers to remove
	 * @param user is the user removing the servers
	 * @param storageSource is reference to the storage source containing the table
	 * */
	public static void removeServers(IStorageSourceService storageSource, int servers, String user) {
		// if number of specified servers >= owned servers, remove all servers
		int max = getServers(storageSource, user);
		servers = (servers > max) ? max : servers;
		
		for(int i=0; i<servers; i++) {
			Map<String,Object> row = new HashMap<String,Object>();
			Integer ID = getLastAssignedServerID(storageSource, user);
			row.put(SDNProject.COLUMN_S_USER, null);
			row.put(SDNProject.COLUMN_S_VIRTUAL, null);
			storageSource.updateRow(SDNProject.TABLE_SERVERS, ID, row);
			log.info("removeServers: removed server " + ID + " to user {}. VIR: {}", user);
		}
	}
	
	/**
	 * finds the ID of the server with highest virtual address assigned to the specified user
	 * @param storageSource is reference to the storage source containing the table
	 * @param user is the user whose servers are to be checked
	 * @return the ID of the server with the highest virtual address
	 * */
	public static int getLastAssignedServerID(IStorageSourceService storageSource, String user) {
		Integer ID = 0;
		String max = "0.0.0.0";
		OperatorPredicate predicate = new OperatorPredicate(SDNProject.COLUMN_S_USER, OperatorPredicate.Operator.EQ, user);
		IResultSet resultSet = storageSource.executeQuery(SDNProject.TABLE_SERVERS, 
			new String[] {SDNProject.COLUMN_S_ID, SDNProject.COLUMN_S_VIRTUAL}, predicate, null);
		Map<String, Object> row;
		
		for (Iterator<IResultSet> it = resultSet.iterator(); it.hasNext(); ) {
			row = it.next().getRow();
			String address = (String) row.get(SDNProject.COLUMN_S_VIRTUAL);
			if(isHigher(address, max)) {
				// address is higher than max
				max = address;
				ID = (Integer) row.get(SDNProject.COLUMN_S_ID);
			}
		}
		
		return ID;
	}
	
	/**
	 * compares two IP addresses
	 * @param newIP is the first IP address
	 * @param oldIP is the second IP address
	 * @return true if newIP is higher than oldIP 
	 * */
	private static boolean isHigher(String newIP, String oldIP) {
		String splittedNew[] = newIP.split("\\.");
		String splittedOld[] = oldIP.split("\\.");
		int oldInt = Integer.parseInt(splittedOld[2]);
		int newInt = Integer.parseInt(splittedNew[2]);
		
		//check penultimate part of the address
		if(newInt > oldInt)
			return true;
		if(newInt < oldInt)
			return false;
		
		//check last part of the address
		oldInt = Integer.parseInt(splittedOld[3]);
		newInt = Integer.parseInt(splittedNew[3]);
		if(newInt > oldInt)
			return true;
		return false;		
	}
	
	
	/**
	 * finds in the servers table the first free server
	 * @param storageSource is reference to the storage source containing the table
	 * @return the ID of the first free server found
	 * */
	public static int getFirstFreeServer(IStorageSourceService storageSource) {
		OperatorPredicate predicate = new OperatorPredicate(SDNProject.COLUMN_S_USER, Operator.EQ, null);
		IResultSet resultSet = storageSource.executeQuery(SDNProject.TABLE_SERVERS, 
				new String[] {SDNProject.COLUMN_S_ID}, predicate, null);
		Map<String, Object> row;
		int ID = -1;
		// return the first result
		try {
			row = resultSet.iterator().next().getRow();
			log.info("getFirstFreeServer: row : " + row.toString());
			ID = (int) row.get(SDNProject.COLUMN_S_ID);
		}
		catch(NoSuchElementException e) {
			log.error("No free servers available!", e);
		}
		
		return ID;
	}
	
	/**
	 * finds the addresses belonging to the specified user
	 * @param storageSource is reference to the storage source containing the table
	 * @param user is the user whose servers are to be checked
	 * @return a list of the addresses belonging to the user
	 * */
	public static List<String> getPoolAddresses(IStorageSourceService storageSource, String user) {
		List<String> addresses = new ArrayList<>();
		OperatorPredicate predicate = new OperatorPredicate(SDNProject.COLUMN_S_USER, OperatorPredicate.Operator.EQ, user);
		IResultSet resultSet = storageSource.executeQuery(SDNProject.TABLE_SERVERS, 
			new String[] {SDNProject.COLUMN_S_PHYSICAL}, predicate, null);
		Map<String, Object> row;
		
		for (Iterator<IResultSet> it = resultSet.iterator(); it.hasNext(); ) {
			row = it.next().getRow();
			String address = (String) row.get(SDNProject.COLUMN_S_PHYSICAL);
			addresses.add(address);
		}
		
		return addresses;
	}
	
	/**
	 * insert a new rule in the rules table
	 * @param storageSource is reference to the storage source containing the table
	 * @param ruleName the name to assign to the rule, must be unique
	 * @param sourceAddress address of the source host
	 * @param destAddress address of the destination host
	 * */
	public static void addToRulesTable(IStorageSourceService storageSource, String ruleName, String sourceAddress, String destAddress) {
		Map<String,Object> row = new HashMap<String,Object>(); 
		row.put(SDNProject.COLUMN_R_NAME, ruleName);
		row.put(SDNProject.COLUMN_R_SRC, sourceAddress);
		row.put(SDNProject.COLUMN_R_DST, destAddress);
		storageSource.insertRowAsync(SDNProject.TABLE_RULES, row);
	}
	
}
