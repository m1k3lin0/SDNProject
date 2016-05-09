package net.floodlightcontroller.sdnproject;

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
	
	@Get("json")
	public String printClients(String jsonData){
		IStorageSourceService storageSource = 
				(IStorageSourceService)getContext().getAttributes().get(IStorageSourceService.class.getCanonicalName());

		/* TODO
		 * parse jsonData to get username
		 * if username doesn't exist in table -> return error
		 * query servers table to find all servers associated to user
		 * return list of server's addresses in json format
		 * */
		Map<String, Object> data = new HashMap<String, Object>();
		String ret = null;
		
		if (jsonData == null) {
			return "{\"status\" : \"Error! No data posted.\"}";
		}
		
		System.out.println("TABLES CREATED: " + storageSource.getAllTableNames());
		
		log.info("received json: " + jsonData);
		
		/* parse json data */
		try {
			data = SDNUtils.jParse(jsonData);
		}
		catch(IOException e) {
			log.error("error while parsing received data: " + jsonData, e);
			return "{\"status\" : \"Error retrieving client info, see log for details.\"}";
		}
		
		String user = (String)data.get(SDNProject.COLUMN_U_NAME); //
		
		/* INSERIMENTI DI PROVA ---> VANNO TOLTI PERCHE' LA TABELLA NON VIENE RIEMPITA QUI */
		/*Map<String,Object> values = new HashMap<String,Object>();
		values.put(SDNProject.COLUMN_S_ID, "ALEXNONECAPACE");
		values.put(SDNProject.COLUMN_S_USER, "trignoleomerda");
		values.put(SDNProject.COLUMN_S_VIRTUAL, "indirizzo di prova");
		Map<String,Object> values2 = new HashMap<String,Object>();
		values2.put(SDNProject.COLUMN_S_ID, "ALEXNONECAPACE2");
		values2.put(SDNProject.COLUMN_S_USER, "trignoleomerda");
		values2.put(SDNProject.COLUMN_S_VIRTUAL, "indirizzo di prova2");
		Map<String,Object> values3 = new HashMap<String,Object>();
		values3.put(SDNProject.COLUMN_S_ID, "ALEXNONECAPACE3");
		values3.put(SDNProject.COLUMN_S_USER, "trignoleomerda");
		values3.put(SDNProject.COLUMN_S_VIRTUAL, "indirizzo di prova3");
		storageSource.insertRowAsync(SDNProject.TABLE_SERVERS, values); //crea una nuova riga nella tabella servers
		storageSource.insertRowAsync(SDNProject.TABLE_SERVERS, values2); //crea una nuova riga nella tabella servers
		storageSource.insertRowAsync(SDNProject.TABLE_SERVERS, values3); //crea una nuova riga nella tabella servers
		/*-----------------------------------------------------------------*/
		
		/* check if username exists */
		if(!SDNUtils.userExists(storageSource, user)) {
			log.error("error while fetching information, user {} not existent!", user);
			return "{\"status\" : \"User not existent, check log for details \"}";
		}
		
		/* create the answer in json format */
		log.info("creating json answer");
		ret = "{ \"user\" : \"" + user + "\", \"servers\" : " + SDNUtils.getServers(storageSource, user) 
				+ ", \"addresses\" : [";
		
		/* query the servers table */
		OperatorPredicate predicate = new OperatorPredicate(SDNProject.COLUMN_S_USER,OperatorPredicate.Operator.EQ,user);
		IResultSet resultSet = storageSource.executeQuery(SDNProject.TABLE_SERVERS, 
			new String[] {SDNProject.COLUMN_S_VIRTUAL}, predicate, null);
		Map<String, Object> row;
		for (Iterator<IResultSet> it = resultSet.iterator(); it.hasNext(); ) {
			row = it.next().getRow();
			ret += "\""+row.get(SDNProject.COLUMN_S_VIRTUAL).toString()+"\"";
			if(it.hasNext()) ret+= ",";
		}
		ret += "] }";
		

		return ret;
	}
	

}