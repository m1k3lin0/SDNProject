package net.floodlightcontroller.sdnproject;

import net.floodlightcontroller.storage.IStorageSourceService;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

//get info about the servers belonging to the client
public class SDNProjectInfoResource extends ServerResource{
	
	@Get("json")
	public String printClients(){
		String[] array = new String[10];
		array[0] = "name";
		IStorageSourceService storageSource = (IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());
		System.out.println("LOLLIPOP" + storageSource.executeQuery("users", array, null, null).toString());
		return storageSource.getRow("users", "name").toString();
		
	}
	

}