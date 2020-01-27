package edu.upc.essi.mongo.designs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

import edu.upc.essi.mongo.util.Const;

public class postgres2mongo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String url = "jdbc:postgresql://10.55.0.32/Author";
		String user = "postgres";
		String password = "user";

		Connection conn;
		try {

			ProcessBuilder p1 = new ProcessBuilder(Const.MONGOD_LOC, "--config", "/root/mongo/mongo10.conf", "--dbpath",
					Const.FOLDER_BASE + "0", "--bind_ip_all", "--fork", "--logpath", Const.LOG_LOC);
			Process p;
			p = p1.start();
			int retval1 = p.waitFor();
			System.out.println(retval1);
			Mongo mongo = new Mongo("localhost", 27017);
			DB db = mongo.getDB("final");
			DBCollection authorcol = db.getCollection("Authors");
			DBCollection bookcol = db.getCollection("Books");
			DBCollection authorbookcol = db.getCollection("Author_Books");

			List<BasicDBObject> documents1 = new ArrayList<>();

			conn = DriverManager.getConnection(url, user, password);
			// TODO Auto-generated catch block

			Statement stmt2 = conn.createStatement();
			String sql2 = "SELECT \"B_ID\", \"B_NAME\"	FROM public.\"Book\";";
			ResultSet rs2 = stmt2.executeQuery(sql2);
			int i = 0;

			while (rs2.next()) {
				i++;
				int id = rs2.getInt("B_ID");
				String name = rs2.getString("B_NAME");

				BasicDBObject updateFields = new BasicDBObject();
				updateFields.append("B_ID", id);
				updateFields.append("B_NAME", name);
				documents1.add(updateFields);

				if (i > 0 && i % 10000 == 0) {
					System.out.println(i);
					if (!documents1.isEmpty()) {
						bookcol.insert(documents1);
						documents1.clear();
					}
				}
			}
			if (!documents1.isEmpty())
				bookcol.insert(documents1);
			documents1.clear();
			rs2.close();

			Statement stmt1 = conn.createStatement();
			String sql1 = "SELECT \"A_ID\", \"A_NAME\"	FROM public.\"Author\";";
			ResultSet rs1 = stmt1.executeQuery(sql1);
			i = 0;

			while (rs1.next()) {
				i++;
				int id = rs1.getInt("A_ID");
				String name = rs1.getString("A_NAME");

				BasicDBObject updateFields = new BasicDBObject();
				updateFields.append("A_ID", id);
				updateFields.append("A_NAME", name);
				documents1.add(updateFields);

				if (i > 0 && i % 10000 == 0) {
					System.out.println(i);
					if (!documents1.isEmpty()) {
						authorcol.insert(documents1);
						documents1.clear();
					}
				}
			}
			if (!documents1.isEmpty())
				authorcol.insert(documents1);
			documents1.clear();
			rs1.close();

			Statement stmt = conn.createStatement();
			String sql = "SELECT \"A_ID\", \"B_ID\"	FROM public.\"Author_Book\";";
			ResultSet rs = stmt.executeQuery(sql);
			i = 0;

			while (rs.next()) {
				i++;
				int id = rs.getInt("A_ID");
				int name = rs.getInt("B_ID");

//				System.out.println(authorcol.findOne(new BasicDBObject("A_ID", id).get("_id")));
				BasicDBObject updateFields = new BasicDBObject();
				updateFields.append("A_ID", id);
				updateFields.append("B_ID", name);
				updateFields.append("authorid", authorcol.findOne(new BasicDBObject("A_ID", id)).get("_id"));
				updateFields.append("bookid", bookcol.findOne(new BasicDBObject("B_ID", name)).get("_id"));
				documents1.add(updateFields);

				if (i > 0 && i % 10000 == 0) {
					System.out.println(i);
					if (!documents1.isEmpty()) {
						authorbookcol.insert(documents1);
						documents1.clear();
					}
				}

			}
			authorbookcol.insert(documents1);
			rs.close();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
