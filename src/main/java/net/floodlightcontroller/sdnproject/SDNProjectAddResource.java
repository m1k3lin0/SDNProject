package net.floodlightcontroller.sdnproject;

import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * responds to URI /add
 * add a number of servers to the pool
 */
public class SDNProjectAddResource extends ServerResource {
	protected static Logger log = LoggerFactory.getLogger(SDNProjectRequestResource.class);
	
	@Post
	public Object add(String jsonData){
		
		/* TODO
		 * parse jsonData to get username and server number
		 * check if username exists
		 * check if servers are available
		 * update users table by adding servers
		 * update servers table
		 * define rules
		 * return OK
		 * */
		
		return null;
	}

}
