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

//first request of a client: create the user and gives him a pool of servers
public class SDNProjectRequestResource extends ServerResource{
	protected static Logger log = LoggerFactory.getLogger(SDNProjectRequestResource.class);
	

	@Post
	public Object request(String fmJson){
		System.out.println("******************************"+fmJson+"*************************** \n");
		IStorageSourceService storageSource = (IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("name", "nomeprova"); //values sono una serie di coppie nome_colonna,valore da inserire nella riga
		storageSource.insertRowAsync("users", values); //crea una nuova riga nella tabella users
		
	//	System.out.println("*************************TIE"+ storageSource.getRowAsync("users", "name").toString() + "\n");

        setStatus(Status.SUCCESS_OK);

        return "{\"status\" : \"success\", \"details\" : \"firewall running\"}";
		
	}
}
