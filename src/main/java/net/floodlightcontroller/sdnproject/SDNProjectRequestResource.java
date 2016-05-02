package net.floodlightcontroller.sdnproject;
//modifica di prova


import java.util.HashMap;
import java.util.Map;

import net.floodlightcontroller.storage.IStorageSourceService;

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
	

	@Post
	public Object request(String jsonData){
		IStorageSourceService storageSource = (IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());
		Map<String,Object> values = new HashMap<String,Object>();
		
		if (log.isDebugEnabled()) {
			log.debug("request received: " + jsonData);
		}
		/* TODO
		 * parse jsonData to get the username and the number of servers requested
		 * check if username already exists in the users table
		 * check if enough servers are available (variable)
		 * update users table
		 * update servers table
		 * define rules
		 * return OK
		 * */
		
		/* values represents the row to be inserted, series of pairs (column_name, value) */
		//values.put(SDNProject.COLUMN_S_USER, "nomeprova");
		//storageSource.insertRowAsync(SDNProject.TABLE_NAME, values); //crea una nuova riga nella tabella users
		
	//	System.out.println("*************************TIE"+ storageSource.getRowAsync("users", "name").toString() + "\n");

        setStatus(Status.SUCCESS_OK);

        return "{\"status\" : \"success\", \"details\" : \"firewall running\"}";
		
	}
}
