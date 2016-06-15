package net.floodlightcontroller.sdnproject.web;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.floodlightcontroller.sdnproject.SDNProject;
import net.floodlightcontroller.sdnproject.SDNUtils;
import net.floodlightcontroller.storage.IStorageSourceService;

import org.restlet.data.Status;
import org.restlet.resource.Post;
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
	 * @param jsonData json that specifies user and servers
	 * */
	@Post
	public Object request(String jsonData){
		/*
		 * parse jsonData to get the username and the number of servers requested
		 * check if username already exists in the users table
		 * check if enough servers are available (variable)
		 * update users table
		 * select free servers to assign to the client
		 * update servers table
		 * define rules
		 * return OK
		 * */
		IStorageSourceService storageSource = (IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());
		Map<String,Object> row = new HashMap<String,Object>();
		
		if (log.isDebugEnabled()) {
			log.debug("request received: " + jsonData);
		}
		
		Map<String, Object> data = new HashMap<String, Object>();
		String user = null;
		int servers = 0;
		
		/* parse jsonData */
		if (jsonData == null) {
			return "{\"status\" : \"Error! No data posted.\"}";
		}
		
		log.info("received json: " + jsonData);
		
		try {
			data = SDNUtils.jParse(jsonData);
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
		if(SDNUtils.userExists(storageSource, user)) {
			log.error("error while creating new user, user {} already existent!", user);
			return "{\"status\" : \"User already existent, see log for details.\"}";
		}
		
		/* fetch free server addresses & update data in servers table */
		SDNUtils.assignServers(storageSource, servers, user);
		
		/* insert data in users table */
		// prepare row
		row.put(SDNProject.COLUMN_U_NAME, user);
		row.put(SDNProject.COLUMN_U_SERVERS, servers);
		// add new row
		storageSource.insertRowAsync(SDNProject.TABLE_USERS, row);
		
		/* new rules defined in SDNProject.rowsModified() */

		SDNProject.available_servers -= servers;
		
        setStatus(Status.SUCCESS_OK);

        return "{\"status\" : \"Success\", \"details\" : \"Run /info to get informations.\"}";
	}

}
