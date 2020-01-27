package edu.upc.essi.mongo.designs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;

public class DataGenerator {

	public static void main(String[] args) {
		String url = "jdbc:postgresql://10.55.0.32/Author";
		String user = "postgres";
		String password = "user";

		Connection conn;
		try {
			conn = DriverManager.getConnection(url, user, password);
			String sql = "INSERT INTO public.\"Author\"(\"A_ID\", \"A_NAME\") VALUES (?, ?);";
			PreparedStatement ps = conn.prepareStatement(sql);
			String sql2 = "INSERT INTO public.\"Book\"(\"B_ID\", \"B_NAME\") VALUES (?, ?);";
			PreparedStatement ps2 = conn.prepareStatement(sql2);

			for (int i = 0; i < 4000000; i++) {
				ps2.setInt(1, i);
				ps2.setString(2, RandomStringUtils.randomAlphanumeric(155));
				ps2.addBatch();
				if (i < 2500000) {
					ps.setInt(1, i);
					ps.setString(2, RandomStringUtils.randomAlphanumeric(105));
					ps.addBatch();
					if (i % 10000 == 0) {
						ps.executeBatch();
						ps = conn.prepareStatement(sql);
					}
				}
				if (i % 10000 == 0) {
					System.out.println(i);
					ps2.executeBatch();
					ps2 = conn.prepareStatement(sql2);

				}
			}
			ps.executeBatch();
			ps2.executeBatch();

			String sql3 = "INSERT INTO public.\"Author_Book\"(\"A_ID\", \"B_ID\") VALUES (?, ?);";
			PreparedStatement ps3 = conn.prepareStatement(sql3);
			for (int i = 0; i < 2500000; i++) {
				int nos = (int) ((Math.random() * ((10 - 1) + 1)) + 1);
				HashSet<Integer> s = new HashSet<Integer>();

				while (s.size() < nos) {
					s.add((int) (Math.random() * ((4000000 - 1) + 1)) + 0);
				}

				for (Integer integer : s) {
					ps3.setInt(1, i);
					ps3.setInt(2, integer);
					ps3.addBatch();
				}

				if (i % 10000 == 0) {
					System.out.println(i);
					ps3.executeBatch();
					ps3 = conn.prepareStatement(sql3);

				}
			}
			ps3.executeBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
