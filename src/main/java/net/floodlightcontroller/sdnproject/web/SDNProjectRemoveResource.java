package net.floodlightcontroller.sdnproject.web;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.restlet.data.Status;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.sdnproject.SDNProject;
import net.floodlightcontroller.sdnproject.SDNUtils;
import net.floodlightcontroller.storage.IStorageSourceService;

/**
 * responds to URI /remove
 * removes a number of servers from the pool
 */
public class SDNProjectRemoveResource extends ServerResource {
	protected static Logger log = LoggerFactory.getLogger(SDNProjectRequestResource.class);
	
	/**
	 * @param jsonData json that specifies user and servers to remove
	 **/
	@Post
	public Object remove(String jsonData){	
		/*
		 * parse jsonData to get username and number of servers
		 * check if user exists
		 * if number > total server -> remove all and delete user from tables
		 * else update tables, update available_servers variable
		 * delete rules
		 * return OK
		 * */
		
		IStorageSourceService storageSource = (IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());
		Map<String,Object> row = new HashMap<String,Object>();
		
		Map<String, Object> data = new HashMap<String, Object>();
		String user = null;
		int servers = 0;
		String ret = null;
		
		/* parse jsonData */
		if (jsonData == null) {
			return "{\"status\" : \"Error! No data posted.\"}";
		}
		
		try {
			data = SDNUtils.jParse(jsonData);
		}
		catch(IOException e) {
			log.error("Error while parsing received data: " + jsonData, e);
			return "{\"status\" : \"Error retrieving client info, see log for details.\"}";
		}
		try {
			user = (String)data.get(SDNProject.COLUMN_U_NAME);
			servers = Integer.parseInt((String)data.get(SDNProject.COLUMN_U_SERVERS));
		}
		catch(NullPointerException e) {
			log.error("Error in the received json data: " + jsonData, e);
			return "{\"status\" : \"Error in json syntax, see log for details.\"}";
		}
		catch(ClassCastException e) {
			log.error("Error in the received json data: " + jsonData, e);
			return "{\"status\" : \"Error in json syntax, see log for details.\"}";
		}
		
		/* check if username exists */
		if(!SDNUtils.userExists(storageSource, user)) {
			log.error("Error while fetching user, user {} not existent!", jsonData);
			return "{\"status\" : \"User not existent, see log for details.\"}";
		}
		
		/* fetch free server addresses & update data in servers table */
		SDNUtils.removeServers(storageSource, servers, user);
		
		/* update users table */
		int owned_servers = SDNUtils.getServers(storageSource, user);
		if(owned_servers <= servers) {
			//delete entry relative to the user
			storageSource.deleteRow(SDNProject.TABLE_USERS, user);
			SDNProject.available_servers += owned_servers;
			ret = "{\"status\" : \"Success\", \"details\" : \"All servers removed from the pool, user "
					+ user + " has been deleted.\"}";
			log.info("Removed all servers from user {}. User has been deleted", user);
		}
		else {
			//decrease owned servers
			row.put(SDNProject.COLUMN_U_SERVERS, owned_servers-servers);
			storageSource.updateRow(SDNProject.TABLE_USERS, user, row);
			SDNProject.available_servers += servers;
			ret = "{\"status\" : \"Success\", \"details\" : \"Servers removed from the pool, user "
					+ user + " now has " + (owned_servers-servers) + " servers. Run /info to get informations.\"}";
			log.info("Removed {} servers from user {}" + SDNUtils.getServers(storageSource, user), user);
		}
		
		/* rules updated in SDNProject.rowsModified() */

        setStatus(Status.SUCCESS_OK);
		
		return ret;
	}
	
	
}
