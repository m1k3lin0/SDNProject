package net.floodlightcontroller.sdnproject;

import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * responds to URI /remove
 * remove a number of servers from the pool
 */
public class SDNProjectRemoveResource extends ServerResource {
	protected static Logger log = LoggerFactory.getLogger(SDNProjectRequestResource.class);
	
	@Post
	public Object remove(String jsonData){
		
		/* TODO
		 * parse jsonData to get username and number of servers
		 * check if user exists
		 * if number is "all" or > total server -> remove all and delete user from tables
		 * else update tables, update available_servers variable
		 * delete rules
		 * return OK
		 * */
		
		return null;
	}
	
	
}
