package edu.upc.essi.mongo.designs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringJoiner;

import org.bson.types.ObjectId;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import edu.upc.essi.mongo.util.CSVUtils;
import edu.upc.essi.mongo.util.Const;

public class designMemory {
	public static void main(String[] args) {

		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
		rootLogger.setLevel(Level.ERROR);
		String Filebase = Const.FOLDER_BASE;
		String idBase = Const.ID_BASE;
		List lAuthors = CSVUtils.fillIds(idBase + "Authors");

		List lBooks = CSVUtils.fillIds(idBase + "Books");

		try {

			for (int k = 0; k < 1; k++) {

				for (int i = 1; i < 8; i++) {
//					if (i == 6)
//						i = 7;
					File dir = new File(Filebase + i);
					dir.mkdir();
					ProcessBuilder p1 = new ProcessBuilder(Const.MONGOD_LOC, "--config", Const.CONFIG_LOC, "--dbpath",
							Filebase + i, "--bind_ip_all", "--fork", "--logpath", Const.LOG_LOC);
					Process p;

					p = p1.start();

					int retval1 = p.waitFor();

					Mongo mongo = new Mongo("localhost", 27017);
					DB db = mongo.getDB("final");// movies
					DBCollection authors = db.getCollection("Authors");
					DBCollection books = db.getCollection("Books");
					DBCollection authorbooks = db.getCollection("Author_Books");
					Random r = new Random();

					FileWriter writer = new FileWriter(Const.ID_BASE + "design" + i + ".txt");
					BufferedWriter buffer = new BufferedWriter(writer);

					if (i == 4) { // authors referred

						for (int j = 0; j < 10000; j++) {
							Thread.sleep(10);
							switch (j % 4) {
							case 0:
								DBCursor n = authors
										.find(new BasicDBObject("_id", lAuthors.get(r.nextInt(lAuthors.size()))));

//								if (n.hasNext()) {
//								String s2 =	(String)
//								DBObject y = n.next();// .get("A_NAME");
								System.out.println(n.next().get("A_NAME"));
//									}
								break;
							case 1:
								DBCursor m = books.find(new BasicDBObject("_id", lBooks.get(r.nextInt(lBooks.size()))));
//								if (m.hasNext()) {
//								String s1 = (String) 
//								m.next();// .get("B_NAME");
								System.out.println(m.next().get("B_NAME"));
//									}
								break;
							case 2:
								DBCursor o = books.find(new BasicDBObject("AUTHORS", new BasicDBObject("$all",
										new ObjectId[] { (ObjectId) lAuthors.get(r.nextInt(lAuthors.size())) })));
								for (DBObject dbObject : o) {
									System.out.println(dbObject.get("B_NAME"));
								}
								break;
							case 3:
								DBCursor q = books.find(new BasicDBObject("_id", lBooks.get(r.nextInt(lBooks.size()))));
								List<ObjectId> l = (List<ObjectId>) (q.next().get("AUTHORS"));

								for (ObjectId objectId : l) {
									synchronized (designMemory.class) {
										ElementbyId(authors, objectId, "A_NAME");
									}
								}
								break;
							default:
								break;
							}
						}

						CommandResult bkresults = books.getStats();
						buffer.write("\n===Books====\n");
						buffer.write(bkresults.toJson());
						CommandResult authresults = authors.getStats();
						buffer.write("\n===Authors====\n");
						buffer.write(authresults.toJson());

					}

					if (i == 5) { // books referred

						for (int j = 0; j < 10000; j++) {
							Thread.sleep(10);
							switch (r.nextInt(4)) {
							case 0: // authors by id
								DBCursor n = authors
										.find(new BasicDBObject("_id", lAuthors.get(r.nextInt(lAuthors.size()))));
//								String s1 = (String)
//								DBObject x = n.next();// .get("A_NAME");
								System.out.println(n.next().get("A_NAME"));
								break;
							case 1: // books by id
								DBCursor m = books.find(new BasicDBObject("_id", lBooks.get(r.nextInt(lBooks.size()))));
//								String s2 = (String) 
//								DBObject x1 = m.next();// .get("B_NAME");
								System.out.println(m.next().get("B_NAME"));
								break;
							case 2: // books by author
								DBCursor q = authors
										.find(new BasicDBObject("_id", lAuthors.get(r.nextInt(lAuthors.size()))));
								List<ObjectId> l = (List<ObjectId>) (q.next().get("BOOKS"));

								for (ObjectId objectId : l) {
									synchronized (designMemory.class) {
										ElementbyId(books, objectId, "B_NAME");
									}
								}

								break;
							case 3: // authors by book
								DBCursor o = authors.find(new BasicDBObject("BOOKS", new BasicDBObject("$all",
										new ObjectId[] { (ObjectId) lBooks.get(r.nextInt(lBooks.size())) })));
								for (DBObject dbObject : o) {
//									String x = (String) 
//									DBObject x3 = dbObject;// .get("A_NAME");
									System.out.println(dbObject.get("A_NAME"));
								}
								break;
							default:
								break;
							}
						}

						CommandResult bkresults = books.getStats();
						buffer.write("\n===Books====\n");
						buffer.write(bkresults.toJson());
						CommandResult authresults = authors.getStats();
						buffer.write("\n===Authors====\n");
						buffer.write(authresults.toJson());

					}

					if (i == 6) { // both referred

						for (int j = 0; j < 10000; j++) {
							Thread.sleep(10);
							switch (r.nextInt(4)) {
							case 0: // author by id
								DBCursor n = authors
										.find(new BasicDBObject("_id", lAuthors.get(r.nextInt(lAuthors.size()))));
//								String s1 = (String)
//								DBObject x = n.next();// .get("A_NAME");/
								System.out.println(n.next().get("A_NAME"));
								break;
							case 1: // book by id
								DBCursor m = books.find(new BasicDBObject("_id", lBooks.get(r.nextInt(lBooks.size()))));
//								String s2 = (String)
//								DBObject x1 = m.next();// .get("B_NAME");
								System.out.println(m.next().get("B_NAME"));
								break;
							case 2: // books by authorid
								DBCursor q = books.find(new BasicDBObject("AUTHORS", new BasicDBObject("$all",
										new ObjectId[] { (ObjectId) lAuthors.get(r.nextInt(lAuthors.size())) })));
								for (DBObject dbObject : q) {
//									String x = (String) 
//									DBObject x2 = dbObject;// .get("B_NAME");
									System.out.println(dbObject.get("B_NAME"));
								}

								break;
							case 3: // authors by bookid
								DBCursor o = authors.find(new BasicDBObject("BOOKS", new BasicDBObject("$all",
										new ObjectId[] { (ObjectId) lBooks.get(r.nextInt(lBooks.size())) })));
								for (DBObject dbObject : o) {
//									String x = (String) 
//									DBObject x3 = dbObject;// .get("A_NAME");
									System.out.println(dbObject.get("A_NAME"));
								}
								break;
							default:
								break;
							}
						}
						CommandResult bkresults = books.getStats();
						buffer.write("\n===Books====\n");
						buffer.write(bkresults.toJson());
						CommandResult authresults = authors.getStats();
						buffer.write("\n===Authors====\n");
						buffer.write(authresults.toJson());

					}

					if (i == 1) { // authors nested

						for (int j = 0; j < 10000; j++) {
							Thread.sleep(10);
							switch (r.nextInt(4)) {
							case 0: // authors by id
								DBObject n = books.findOne(
										new BasicDBObject("AUTHORS._id", lAuthors.get(r.nextInt(lAuthors.size()))));
//								String s1 = (String) n;//.get("A_NAME");
//								n.get("AUTHORS");
								System.out.println(n.get("AUTHORS"));
								break;
							case 1: // book by id
								DBCursor m = books.find(new BasicDBObject("_id", lBooks.get(r.nextInt(lBooks.size()))));
//								String s2 = (String)
//								DBObject x = m.next();// .get("B_NAME");
								System.out.println(m.next().get("B_NAME"));
								break;
							case 2: // books by author
								DBCursor q = books.find(
										new BasicDBObject("AUTHORS._id", lAuthors.get(r.nextInt(lAuthors.size()))));
								for (DBObject dbObject : q) {
									System.out.println(dbObject.get("B_NAME"));
								}

								break;
							case 3: // authors by book
								DBCursor o = books.find(new BasicDBObject("_id", lBooks.get(r.nextInt(lBooks.size()))));
//								Object s = 
//								DBObject x2 = o.next();
								System.out.println(o.next().get("AUTHORS"));
								break;
							default:
								break;
							}
						}

						CommandResult bkresults = books.getStats();
						buffer.write("\n===Books====\n");
						buffer.write(bkresults.toJson());

					}

					if (i == 2) { // books nested

						for (int j = 0; j < 10000; j++) {
							Thread.sleep(10);
							switch (r.nextInt(4)) {
							case 0: // authors by id
								DBCursor n = authors
										.find(new BasicDBObject("_id", lAuthors.get(r.nextInt(lAuthors.size()))));
//								String s1 = (String)
//								DBObject x = n.next();// .get("A_NAME");
								System.out.println(n.next().get("A_NAME"));
								break;
							case 1: // book by id
//								Object y = ;
//								System.out.println(y);
								DBObject m = authors
										.findOne(new BasicDBObject("BOOKS._id", lBooks.get(r.nextInt(lBooks.size()))));
//								Object s2 =
//								m.get("BOOKS");
								System.out.println(m.get("BOOKS"));
								break;
							case 2: // books by author
								DBCursor q = authors
										.find(new BasicDBObject("_id", lAuthors.get(r.nextInt(lAuthors.size()))));
//								DBObject x1 = q.next();
//										.get("BOOKS");
								System.out.println(q.next().get("BOOKS"));
								break;
							case 3: // authors by book
								DBCursor o = authors
										.find(new BasicDBObject("BOOKS._id", lBooks.get(r.nextInt(lBooks.size()))));
								for (DBObject dbObject : o) {
//									DBObject x3 = dbObject;// .get("A_NAME");
									System.out.println(dbObject.get("A_NAME"));
								}
								break;
							default:
								break;
							}
						}

						CommandResult authresults = authors.getStats();
						buffer.write("\n===Authors====\n");
						buffer.write(authresults.toJson());

					}

					if (i == 3) { // both nested

						for (int j = 0; j < 10000; j++) {
							Thread.sleep(10);
							switch (r.nextInt(4)) {
							case 0: // authors by id
								DBCursor n = authors
										.find(new BasicDBObject("_id", lAuthors.get(r.nextInt(lAuthors.size()))));
//								String s1 = (String) 
//								DBObject x = n.next();// .get("A_NAME");
								System.out.println(n.next().get("A_NAME"));
								break;
							case 1: // book by id
								DBCursor m = books.find(new BasicDBObject("_id", lBooks.get(r.nextInt(lBooks.size()))));
//								String s2 = (String) 
//								DBObject x1 = m.next();// .get("B_NAME");
								System.out.println(m.next().get("B_NAME"));
								break;
							case 2: // books by author
								DBCursor q = authors
										.find(new BasicDBObject("_id", lAuthors.get(r.nextInt(lAuthors.size()))));
//								Object s3 =
//								DBObject x3 = q.next();
								System.out.println(q.next().get("BOOKS"));
								break;
							case 3: // authors by book
								DBCursor o = books.find(new BasicDBObject("_id", lBooks.get(r.nextInt(lBooks.size()))));
//								Object s4 = 
//								DBObject x4 = o.next();// .get("AUTHORS");
								System.out.println(o.next().get("AUTHORS"));
								break;
							default:
								break;
							}
						}

						CommandResult bkresults = books.getStats();
						buffer.write("\n===Books====\n");
						buffer.write(bkresults.toJson());
						CommandResult authresults = authors.getStats();
						buffer.write("\n===Authors====\n");
						buffer.write(authresults.toJson());

					}
					if (i == 7) { // 3NF nested

						for (int j = 0; j < 10000; j++) {
							Thread.sleep(10);
							switch (r.nextInt(4)) {
							case 0: // authors by id
								DBCursor n = authors
										.find(new BasicDBObject("_id", lAuthors.get(r.nextInt(lAuthors.size()))));
//								String s1 = (String) 
//								DBObject x = n.next();// .get("A_NAME");
								System.out.println(n.next().get("A_NAME"));
								break;
							case 1: // book by id
								DBCursor m = books.find(new BasicDBObject("_id", lBooks.get(r.nextInt(lBooks.size()))));
//								String s2 = (String) 
//								DBObject x1 = m.next();// .get("B_NAME");
								System.out.println(m.next().get("B_NAME"));
								break;
							case 2: // books by author
								DBCursor q = authorbooks
										.find(new BasicDBObject("authorid", lAuthors.get(r.nextInt(lAuthors.size()))));
								for (DBObject dbObject : q) {
									synchronized (designMemory.class) {
										BookbyDBObject(books, dbObject, "B_NAME");
									}
								}
								break;
							case 3: // authors by book
								DBCursor o = authorbooks
										.find(new BasicDBObject("bookid", lBooks.get(r.nextInt(lBooks.size()))));
								for (DBObject dbObject : o) {
									synchronized (designMemory.class) {
										AuthorbyDBObject(authors, dbObject, "A_NAME");
									}
								}
								break;
							default:
								break;
							}
						}
						CommandResult bkresults = books.getStats();
						buffer.write("\n===Books====\n");
						buffer.write(bkresults.toJson());
						CommandResult authresults = authors.getStats();
						buffer.write("\n===Authors====\n");
						buffer.write(authresults.toJson());
						CommandResult abResult = authorbooks.getStats();
						buffer.write("\n===AuthorBooks====\n");
						buffer.write(abResult.toJson());
					}
					buffer.close();
					mongo.close();
					ProcessBuilder p2 = new ProcessBuilder("mongo", "localhost:27017/admin", "--eval",
							"db.shutdownServer()");
					Process p3 = p2.start();
					int retval2 = p3.waitFor();
					System.out.println("shutting down" + retval2);
					ProcessBuilder p21 = new ProcessBuilder(Const.CLEAR);
					Process p31 = p21.start();
//			int retval2 = p3.waitFor();
//			System.out.println(p2.command());
//			System.out.println(retval2);

					BufferedReader reader = new BufferedReader(new InputStreamReader(p31.getInputStream()));
					StringJoiner sj = new StringJoiner(System.getProperty("line.separator"));
					reader.lines().iterator().forEachRemaining(sj::add);
					String xresult = sj.toString();
					int retvalx = p31.waitFor();
					System.out.println(xresult);

				}
			}

		} catch (

		Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static synchronized void BookbyDBObject(DBCollection books, DBObject dbObject, String name) {
		DBCursor s = books.find(new BasicDBObject("_id", dbObject.get("bookid")));
//									String s3 = (String) 
//		DBObject x2 = s.next();// .get("B_NAME");
		System.out.println(s.next().get(name));
	}

	private static synchronized void AuthorbyDBObject(DBCollection col, DBObject dbObject, String name) {
		DBCursor s = col.find(new BasicDBObject("_id", dbObject.get("authorid")));
//									String s4 = (String) 
//		DBObject x3 = s.next();// .get("A_NAME");
		System.out.println(s.next().get(name));
	}

	private static synchronized void ElementbyId(DBCollection col, ObjectId objectId, String name) {
		DBCursor s = col.find(new BasicDBObject("_id", objectId));
		System.out.println(s.next().get(name));
	}
}
