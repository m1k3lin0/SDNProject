package net.floodlightcontroller.sdnproject;
//modifica di prova


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.floodlightcontroller.storage.IPredicate;
import net.floodlightcontroller.storage.IQuery;
import net.floodlightcontroller.storage.IResultSet;
import net.floodlightcontroller.storage.IStorageSourceService;
import net.floodlightcontroller.storage.OperatorPredicate;
import net.floodlightcontroller.storage.OperatorPredicate.Operator;

import org.apache.derby.impl.sql.compile.Predicate;
import org.restlet.data.Status;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * responds to URI /request
 * it is the first request of the client: add new user to users table and assign a pool of servers
 */
public class SDNProjectRequestResource extends ServerResource{
	protected static Logger log = LoggerFactory.getLogger(SDNProjectRequestResource.class);
	
	/**
	 * @param jsonData: json that specifies user and servers
	 * */
	@Post
	public Object request(String jsonData){
		IStorageSourceService storageSource = (IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());
		Map<String,Object> row = new HashMap<String,Object>();
		
		if (log.isDebugEnabled()) {
			log.debug("request received: " + jsonData);
		}
		/* TODO
		 * parse jsonData to get the username and the number of servers requested
		 * check if username already exists in the users table
		 * check if enough servers are available (variable)
		 * update users table
		 * select free servers to assign to the client
		 * update servers table
		 * define rules
		 * return OK
		 * */
		Map<String, Object> data = new HashMap<String, Object>();
		String user = null;
		int servers = 0;
		
		/* parse jsonData */
		if (jsonData == null) {
			return "{\"status\" : \"Error! No data posted.\"}";
		}
		
		log.info("received json: " + jsonData);
		
		try {
			data = SDNProject.jParse(jsonData);
		}
		catch(IOException e) {
			log.error("error while parsing received data: " + jsonData, e);
			return "{\"status\" : \"Error retrieving client info, see log for details.\"}";
		}
		try {
			user = (String)data.get(SDNProject.COLUMN_U_NAME);
			servers = Integer.parseInt((String)data.get(SDNProject.COLUMN_U_SERVERS));
		}
		catch(NullPointerException e) {
			log.error("error in the received json data: " + jsonData, e);
			return "{\"status\" : \"Error in json syntax, see log for details.\"}";
		}
		
		/* check if enough servers are available */
		if(servers > SDNProject.available_servers) {
			log.error("error while assigning requested servers, only " + SDNProject.available_servers + " are available");
			return "{\"status\" : \"Not enough servers available, see log for details.\"}";
		}
		
		/* check if username already existent */
		if(userExists(SDNProject.TABLE_USERS, user)) {
			log.error("error while creating new user, user {} already existent!", user);
			return "{\"status\" : \"User already existent, see log for details.\"}";
		}
		
		/* insert data in users table */
		// prepare row
		row.put(SDNProject.COLUMN_U_NAME, user);
		row.put(SDNProject.COLUMN_U_SERVERS, servers);
		// add new row
		storageSource.insertRowAsync(SDNProject.TABLE_USERS, row);
		
		/* TODO fetch free server adrresses & update data in servers table */

		/* TODO define new rules */
		
        setStatus(Status.SUCCESS_OK);

        return "{\"status\" : \"Success\", \"details\" : \"Run /info to get informations\"}";
		
	}
	
	/**
	 * check if a user is already in the table
	 * @param tableName: name of the table to check in
	 * @param user:		 name of the user
	 * @return true if the user already exists in the table
	 * */
	private boolean userExists(String tableName, String user){
		IStorageSourceService storageSource = (IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());
		OperatorPredicate predicate = new OperatorPredicate(SDNProject.COLUMN_U_NAME, Operator.EQ, user);
		IResultSet resultSet = storageSource.executeQuery(tableName, new String[] {SDNProject.COLUMN_U_NAME}, predicate, null);
		if(resultSet.iterator().hasNext())
			return true;
		return false;
		
	}
}
