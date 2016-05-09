package net.floodlightcontroller.sdnproject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.restlet.data.Status;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.storage.IStorageSourceService;

/**
 * responds to URI /add
 * add a number of servers to the pool
 */
public class SDNProjectAddResource extends ServerResource {
	protected static Logger log = LoggerFactory.getLogger(SDNProjectRequestResource.class);
	
	/**
	 * @param jsonData: json that specifies user and servers
	 * */
	@Post
	public Object add(String jsonData){
		IStorageSourceService storageSource = (IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());
		Map<String,Object> row = new HashMap<String,Object>();
		
		if (log.isDebugEnabled()) {
			log.debug("request received: " + jsonData);
		}
		/* TODO
		 * parse jsonData to get username and server number
		 * check if username exists
		 * check if servers are available
		 * update users table by adding servers
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
		
		/* check if username exists */
		if(!SDNUtils.userExists(storageSource, user)) {
			log.error("error while fetching user, user {} not existent!", jsonData);
			return "{\"status\" : \"User not existent, see log for details.\"}";
		}

		/* fetch free server addresses & update data in servers table */
		SDNUtils.assignServers(storageSource, servers, user);
		
		/* update users table */
		int tot_servers = SDNUtils.getServers(storageSource, user) + servers;
		row.put(SDNProject.COLUMN_U_SERVERS, tot_servers);
		storageSource.updateRow(SDNProject.TABLE_USERS, user, row);
		
		log.info("new value of servers for user {}: " + SDNUtils.getServers(storageSource, user), user);

		/* TODO define new rules */

		SDNProject.available_servers -= servers;

        setStatus(Status.SUCCESS_OK);
		
		return "{\"status\" : \"Success\", \"details\" : \"New servers added to the pool, user "
			+ user + " now has " + tot_servers + " servers. Run /info to get informations.\"}";
	}

}
