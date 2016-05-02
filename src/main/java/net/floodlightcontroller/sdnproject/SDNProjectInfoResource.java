package net.floodlightcontroller.sdnproject;

import net.floodlightcontroller.storage.IStorageSourceService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * responds to URI /info
 * returns the list of server belonging to the client
 */
public class SDNProjectInfoResource extends ServerResource {
	protected static Logger log = LoggerFactory.getLogger(SDNProjectRequestResource.class);
	
	@Get("json")
	public String printClients(String jsonData){
		IStorageSourceService storageSource = 
				(IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());
		
		Map<String, Object> data = new HashMap<String, Object>();
		String ret = null;
		
		if (jsonData == null) {
			return "{\"status\" : \"Error! No data posted.\"}";
		}
		
		System.out.println("\n ***RECEIVED: " + jsonData + "\n");
		
		try {
			data = SDNProject.jParse(jsonData);
		}
		catch(IOException e) {
			log.error("error while parsing received data: " + jsonData, e);
			return "{\"status\" : \"Error retrieving client info, see log for details.\"}";
		}
		
		ret = 	"{ \"user\" : \" " + data.get(SDNProject.COLUMN_U_NAME) + "\","
				+ "\"servers\" : \" " + data.get(SDNProject.COLUMN_U_SERVERS) + "\""
				+ "}";
		/* TODO
		 * parse jsonData to get username
		 * if username doesn't exist in table -> return error
		 * query servers table to find all servers associated to user
		 * return list of server's addresses in json format
		 * */

		return ret;
	}
	

}