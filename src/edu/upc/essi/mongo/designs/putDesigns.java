package edu.upc.essi.mongo.designs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import edu.upc.essi.mongo.util.Const;

public class putDesigns {
	public static void main(String[] args) {

		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
		rootLogger.setLevel(Level.ERROR);
		String Filebase = "/root/mongo/data/author/design";
		try {
			for (int i = 1; i < 8; i++) {
				File dir = new File(Filebase + i);
				dir.mkdir();

				// Running on another port
				ProcessBuilder p1 = new ProcessBuilder(Const.MONGOD_LOC, "--config", "/root/mongo/mongo10port.conf",
						"--dbpath", Filebase + i, "--bind_ip_all", "--fork", "--logpath", "/root/log/mongodb.log");

				Process p;

				p = p1.start();

				int retval1 = p.waitFor();
				System.out.println(retval1);

				Mongo mongo = new Mongo("localhost", 27017);
				DB db = mongo.getDB("final");// movies
				DBCollection authors = db.getCollection("Authors");
				DBCollection books = db.getCollection("Books");
				DBCollection authorbooks = db.getCollection("Author_Books");

				Mongo mongo2 = new Mongo("localhost", 27018);
				DB db2 = mongo2.getDB("final");// movies
				DBCollection iauthors = db2.getCollection("Authors");
				DBCollection ibooks = db2.getCollection("Books");
				DBCollection iauthorbooks = db2.getCollection("Author_Books");

				if (i == 7) { // 3NF

					books.find().forEach(book -> {
						ibooks.insert(book);
					});
					authors.find().forEach(author -> {
						iauthors.insert(author);
					});
					authorbooks.find().forEach(ab -> {
						BasicDBObject obj = new BasicDBObject();
						obj.append("authorid", ab.get("authorid"));
						obj.append("bookid", ab.get("bookid"));
						iauthorbooks.insert(obj);
					});

					Thread.sleep(1000);
				}

				if (i == 4) { // authors referred
					books.find().forEach(book -> {
						BasicDBObject obj = new BasicDBObject();
						obj.append("_id", book.get("_id"));
						obj.append("B_ID", book.get("B_ID"));
						obj.append("B_NAME", book.get("B_NAME"));

						List<ObjectId> aths = new ArrayList<>();

						authorbooks.find(new BasicDBObject().append("B_ID", book.get("B_ID"))).forEach(ab -> {
							aths.add((ObjectId) ab.get("authorid"));
						});

						obj.put("AUTHORS", aths);
						ibooks.insert(obj);
					});
					authors.find().forEach(author -> {
						iauthors.insert(author);
					});

				}

				if (i == 5) { // books referred
					books.find().forEach(book -> {
						ibooks.insert(book);
					});

					authors.find().forEach(author -> {

						BasicDBObject obj = new BasicDBObject();
						obj.append("_id", author.get("_id"));
						obj.append("A_ID", author.get("A_ID"));
						obj.append("A_NAME", author.get("A_NAME"));

						List<ObjectId> bks = new ArrayList<>();

						authorbooks.find(new BasicDBObject().append("A_ID", author.get("A_ID"))).forEach(ab -> {
							bks.add((ObjectId) ab.get("bookid"));
						});

						obj.put("BOOKS", bks);
						iauthors.insert(obj);
					});
				}

				if (i == 6) { // both referred
					books.find().forEach(book -> {
						BasicDBObject obj = new BasicDBObject();
						obj.append("_id", book.get("_id"));
						obj.append("B_ID", book.get("B_ID"));
						obj.append("B_NAME", book.get("B_NAME"));

						List<ObjectId> aths = new ArrayList<>();

						authorbooks.find(new BasicDBObject().append("B_ID", book.get("B_ID"))).forEach(ab -> {
							aths.add((ObjectId) ab.get("authorid"));
						});

						obj.put("AUTHORS", aths);
						ibooks.insert(obj);
					});

					authors.find().forEach(author -> {

						BasicDBObject obj = new BasicDBObject();
						obj.append("_id", author.get("_id"));
						obj.append("A_ID", author.get("A_ID"));
						obj.append("A_NAME", author.get("A_NAME"));

						List<ObjectId> bks = new ArrayList<>();

						authorbooks.find(new BasicDBObject().append("A_ID", author.get("A_ID"))).forEach(ab -> {
							bks.add((ObjectId) ab.get("bookid"));
						});

						obj.put("BOOKS", bks);
						iauthors.insert(obj);
					});
				}

				if (i == 1) { // authors nested
					books.find().forEach(book -> {
						BasicDBObject obj = new BasicDBObject();
						obj.append("_id", book.get("_id"));
						obj.append("B_ID", book.get("B_ID"));
						obj.append("B_NAME", book.get("B_NAME"));

						List<DBObject> aths = new ArrayList<>();

						authorbooks.find(new BasicDBObject().append("B_ID", book.get("B_ID"))).forEach(ab -> {
							aths.add(authors.findOne(new BasicDBObject().append("_id", ab.get("authorid"))));
						});

						obj.put("AUTHORS", aths);
						ibooks.insert(obj);
					});
				}

				if (i == 2) { // books nested

					authors.find().forEach(author -> {

						BasicDBObject obj = new BasicDBObject();
						obj.append("_id", author.get("_id"));
						obj.append("A_ID", author.get("A_ID"));
						obj.append("A_NAME", author.get("A_NAME"));

						List<DBObject> bks = new ArrayList<>();

						authorbooks.find(new BasicDBObject().append("A_ID", author.get("A_ID"))).forEach(ab -> {
							bks.add(books.findOne(new BasicDBObject().append("_id", ab.get("bookid"))));
						});

						obj.put("BOOKS", bks);
						iauthors.insert(obj);
					});
				}

				if (i == 3) { // both nested
					authors.find().forEach(author -> {

						BasicDBObject obj = new BasicDBObject();
						obj.append("_id", author.get("_id"));
						obj.append("A_ID", author.get("A_ID"));
						obj.append("A_NAME", author.get("A_NAME"));

						List<DBObject> bks = new ArrayList<>();

						authorbooks.find(new BasicDBObject().append("A_ID", author.get("A_ID"))).forEach(ab -> {
							bks.add(books.findOne(new BasicDBObject().append("_id", ab.get("bookid"))));
						});

						obj.put("BOOKS", bks);
						iauthors.insert(obj);
					});

					books.find().forEach(book -> {
						BasicDBObject obj = new BasicDBObject();
						obj.append("_id", book.get("_id"));
						obj.append("B_ID", book.get("B_ID"));
						obj.append("B_NAME", book.get("B_NAME"));

						List<DBObject> aths = new ArrayList<>();

						authorbooks.find(new BasicDBObject().append("B_ID", book.get("B_ID"))).forEach(ab -> {
							aths.add(authors.findOne(new BasicDBObject().append("_id", ab.get("authorid"))));
						});

						obj.put("AUTHORS", aths);
						ibooks.insert(obj);
					});
				}

				mongo.close();
				mongo2.close();
				Thread.sleep(120000);
				ProcessBuilder p2 = new ProcessBuilder("mongo", "localhost:27018/admin", "--eval",
						"db.shutdownServer()");
				Process p3 = p2.start();
				int retval2 = p3.waitFor();

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
