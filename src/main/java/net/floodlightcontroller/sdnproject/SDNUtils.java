package net.floodlightcontroller.sdnproject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
	 * @param storageSource: reference to the storage source containing the table
	 * @param user:	name of the user
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
	 * @param storageSource: reference to the storage source containing the table
	 * @param user: name of the user
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
			log.info("row : " + row.toString());
			servers = (int)row.get(SDNProject.COLUMN_U_SERVERS);
		}
		catch(NoSuchElementException e) {
			log.error("No result for user {}", user, e);
		}
		
		return servers;
	}
}