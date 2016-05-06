package net.floodlightcontroller.sdnproject;

import net.floodlightcontroller.storage.IPredicate;
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
		
		Map<String, Object> data = new HashMap<String, Object>();
		String ret = null;
		
		if (jsonData == null) {
			return "{\"status\" : \"Error! No data posted.\"}";
		}
		
		System.out.println("TABLES CREATED: " + storageSource.getAllTableNames());
		
		log.info("received json: " + jsonData);
		
		try {
			data = SDNProject.jParse(jsonData);
		}
		catch(IOException e) {
			log.error("error while parsing received data: " + jsonData, e);
			return "{\"status\" : \"Error retrieving client info, see log for details.\"}";
		}
		String client = (String)data.get(SDNProject.COLUMN_U_NAME); //
		OperatorPredicate predicate = new OperatorPredicate(SDNProject.COLUMN_S_USER,OperatorPredicate.Operator.EQ,client);
		
		Map<String,Object> values = new HashMap<String,Object>();
		values.put(SDNProject.COLUMN_S_ID, "ALEXNONECAPACE");
		values.put(SDNProject.COLUMN_S_USER, "trignoleomerda");
		values.put(SDNProject.COLUMN_S_VIRTUAL, "indirizzo di prova");
		storageSource.insertRowAsync(SDNProject.TABLE_SERVERS, values); //crea una nuova riga nella tabella users
		
		
//		Map<String,Object> row;
		/*ret = 	"{ \"user\" : \" " + data.get(SDNProject.COLUMN_U_NAME) + "\","
				+ "\"servers\" : \" " + data.get(SDNProject.COLUMN_U_SERVERS) + "\""
				+ "}";
		/* TODO
		 * parse jsonData to get username
		 * if username doesn't exist in table -> return error
		 * query servers table to find all servers associated to user
		 * return list of server's addresses in json format
		 * */
		
		IResultSet resultSet = storageSource.executeQuery(SDNProject.TABLE_SERVERS, 
			new String[] {SDNProject.COLUMN_S_VIRTUAL}, predicate, null);
		Map<String, Object> row;
		int i=0;
		for (Iterator<IResultSet> it = resultSet.iterator(); it.hasNext(); i++) {
			row = it.next().getRow();
			log.info("PORCO DUEEEEEEEEEEEEEEEEEEEEEEEEEEEE **** ROW(" + i + ") = " + row.get(SDNProject.COLUMN_S_VIRTUAL).toString());
		}
		
		

		return ret;
	}
	

}