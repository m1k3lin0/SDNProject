package net.floodlightcontroller.sdnproject;

import net.floodlightcontroller.storage.IResultSet;
import net.floodlightcontroller.storage.IStorageSourceService;
import net.floodlightcontroller.storage.OperatorPredicate;
import net.floodlightcontroller.storage.OperatorPredicate.Operator;

public final class SDNUtils {

	/**
	 * check if a user is already in the users table
	 * @param storageSource: reference to the storage source containing the table
	 * @param user:	name of the user
	 * @return true if the user already exists in the table
	 * */
	public static boolean userExists(IStorageSourceService storageSource, String user){
		OperatorPredicate predicate = new OperatorPredicate(SDNProject.COLUMN_U_NAME, Operator.EQ, user);
		IResultSet resultSet = storageSource.executeQuery(SDNProject.TABLE_USERS, new String[] {SDNProject.COLUMN_U_NAME}, predicate, null);
		if(resultSet.iterator().hasNext())
			return true;
		return false;	
	}
	
}
