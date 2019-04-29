package com.mym.test;

import java.net.UnknownHostException;
import java.util.Set;

import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.Mongo;

public class TConnection {
	@Test
	public void testConnection() {
		try {
			Mongo mongo = new Mongo("192.168.31.201",27017);
			
			DB db = mongo.getDB("Test");
			Set<String> collectionNames = db.getCollectionNames();
			for(String name:collectionNames) {
				System.out.println(name);
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
