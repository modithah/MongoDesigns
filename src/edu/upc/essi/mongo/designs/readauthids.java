package edu.upc.essi.mongo.designs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

public class readauthids {
	public static void main(String[] args) {

		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
		rootLogger.setLevel(Level.ERROR);
		String idbase = "/root/mongo/data/author/";
		try {


			Mongo mongo = new Mongo("localhost", 27017);
			DB db = mongo.getDB("final");// movies
			DBCollection authors = db.getCollection("Author");
			DBCollection books = db.getCollection("Book");
			DBCollection authorbooks = db.getCollection("Author_Book");

			ArrayList al = new ArrayList();
			DBCursor x = authors.find(new BasicDBObject(), new BasicDBObject("_id", 1));
			for (DBObject dbObject : x) {
				al.add(dbObject.get("_id"));
			}
			System.out.println(al.size());
			// ArrayList al = new ArrayList()
			// do something with your ArrayList
			FileOutputStream fos;
			fos = new FileOutputStream(idbase + "Authors");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(al);
			oos.close();
			
			ArrayList al2 = new ArrayList();
			DBCursor x2 = books.find(new BasicDBObject(), new BasicDBObject("_id", 1));
			for (DBObject dbObject : x2) {
				al2.add(dbObject.get("_id"));
			}
			System.out.println(al2.size());
			// ArrayList al = new ArrayList()
			// do something with your ArrayList
			FileOutputStream fos2;
			fos2 = new FileOutputStream(idbase + "Books");
			ObjectOutputStream oos2 = new ObjectOutputStream(fos2);
			oos2.writeObject(al2);
			oos2.close();

		
			mongo.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
