package net.floodlightcontroller.sdnproject.web;

import net.floodlightcontroller.sdnproject.SDNProject;
import net.floodlightcontroller.sdnproject.SDNUtils;
import net.floodlightcontroller.storage.IResultSet;
import net.floodlightcontroller.storage.IStorageSourceService;
import net.floodlightcontroller.storage.OperatorPredicate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
	
	/**
	 * @param jsonData json that specifies user
	 * */
	@Get("json")
	public String printClients(String jsonData){
		/*
		 * parse jsonData to get username
		 * if username doesn't exist in table -> return error
		 * query servers table to find all servers associated to user
		 * return list of server's addresses in json format
		 * */
		IStorageSourceService storageSource = (IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());
		
		Map<String, Object> data = new HashMap<String, Object>();
		String user = null;
		String ret = null;
		
		if (jsonData == null) {
			return "{\"status\" : \"Error! No data posted.\"}";
		}
		
		/* parse json data */
		try {
			data = SDNUtils.jParse(jsonData);
		}
		catch(IOException e) {
			log.error("Error while parsing received data: " + jsonData, e);
			return "{\"status\" : \"Error retrieving client info, see log for details.\"}";
		}
		
		try {
			user = (String)data.get(SDNProject.COLUMN_U_NAME);
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
			log.error("Error while fetching user, user {} not existent!", user);
			return "{\"status\" : \"User not existent, see log for details \"}";
		}
		
		/* create the answer in json format */
		ret = "{ \"user\" : \"" + user + "\", \"servers\" : " + SDNUtils.getServers(storageSource, user) 
				+ ", \"addresses\" : [";
		
		/* query the servers table */
		OperatorPredicate predicate = new OperatorPredicate(SDNProject.COLUMN_S_USER, OperatorPredicate.Operator.EQ, user);
		IResultSet resultSet = storageSource.executeQuery(SDNProject.TABLE_SERVERS, 
				new String[] {SDNProject.COLUMN_S_PUBLIC}, predicate, null);
		Map<String, Object> row;
		for (Iterator<IResultSet> it = resultSet.iterator(); it.hasNext(); ) {
			row = it.next().getRow();
			ret += "\"" + row.get(SDNProject.COLUMN_S_PUBLIC).toString() + "\"";
			if(it.hasNext())
				ret += ",";
		}
		ret += "] }";
		
		return ret;
	}
}