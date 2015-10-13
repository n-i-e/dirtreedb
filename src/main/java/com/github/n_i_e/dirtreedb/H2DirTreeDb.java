/*
 * Copyright 2015 Namihiko Matsumura (https://github.com/n-i-e/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.n_i_e.dirtreedb;

import java.io.File;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

public class H2DirTreeDb extends CommonSqlDirTreeDb {
	public H2DirTreeDb(String filename) throws ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");

		assert(filename.substring(filename.length()-6).equalsIgnoreCase(".mv.db"));
		File fileobj = new File(filename);
		boolean fileExists = fileobj.exists();

		String f = filename.substring(0, filename.length()-6);
		conn = DriverManager.getConnection("jdbc:h2:" + f + ";COMPRESS=TRUE");
		conn.setAutoCommit(true);
		if (!fileExists) {
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS directory (pathid BIGINT AUTO_INCREMENT PRIMARY KEY, "
						+ "parentid BIGINT NOT NULL, rootid BIGINT, datelastmodified TIMESTAMP NOT NULL, "
						+ "size BIGINT NOT NULL, compressedsize BIGINT NOT NULL, csum BIGINT, "
						+ "path VARCHAR UNIQUE NOT NULL, type INTEGER NOT NULL, status INTEGER NOT NULL, "
						+ "duplicate BIGINT NOT NULL, dedupablesize BIGINT NOT NULL, "
						+ "CONSTRAINT pathid_size_csum UNIQUE (pathid, size, csum))");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_parentid ON directory (parentid)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_rootid ON directory (rootid)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_datelastmodified ON directory (datelastmodified)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_size ON directory (size)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_compressedsize ON directory (compressedsize)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_csum ON directory (csum)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_size_csum ON directory (size, csum)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_type ON directory (type)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_status ON directory (status)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_duplicate ON directory (duplicate)");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS directory_dedupablesize ON directory (dedupablesize)");

				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS upperlower (upper BIGINT NOT NULL, "
						+ "lower BIGINT NOT NULL, distance INTEGER NOT NULL, PRIMARY KEY (upper, lower), "
						+ "FOREIGN KEY (upper) REFERENCES directory (pathid), "
						+ "FOREIGN KEY (lower) REFERENCES directory (pathid))");
				stmt.executeUpdate("CREATE INDEX IF NOT EXISTS upperlower_distance ON upperlower (distance)");

				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS equality (pathid1 BIGINT, pathid2 BIGINT, "
						+ "size BIGINT NOT NULL, csum BIGINT NOT NULL, "
						+ "datelasttested TIMESTAMP NOT NULL, PRIMARY KEY (pathid1, pathid2), "
						+ "FOREIGN KEY (pathid1, size, csum) REFERENCES directory (pathid, size, csum), "
						+ "FOREIGN KEY (pathid2, size, csum) REFERENCES directory (pathid, size, csum))");

				PreparedStatement ps = conn.prepareStatement("INSERT INTO directory (path, parentid, "
						+ "datelastmodified, size, compressedsize, type, status, duplicate, dedupablesize) "
						+ "VALUES ('C:\\', 0, ?, 0, 0, 0, 1, 0, 0)");
				try {
					ps.setTimestamp(1, new Timestamp(0L));
					ps.executeUpdate();
				} finally {
					ps.close();
				}
				stmt.executeUpdate("UPDATE directory SET rootid=pathid WHERE rootid IS NULL AND pathid IS NOT NULL");
			} finally {
				stmt.close();
			}
		}
	}
}
