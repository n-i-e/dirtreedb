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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MdbDirTreeDb extends CommonSqlDirTreeDb {
	MdbDirTreeDb(String filename) throws ClassNotFoundException, SQLException, IOException {
		Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");

		File fileobj = new File(filename);
		if (! fileobj.exists()) {
			InputStream inf=getClass().getResourceAsStream("/com/github/n_i_e/dirtreedb/binary/initialMDB.mdb");
			if (inf == null) { throw new IOException("!! Cannot access to resource nullMDB.mdb"); }
			OutputStream outf = new FileOutputStream(filename);

			byte[] buff = new byte[4096];
			int len;
			while ((len = inf.read(buff, 0, buff.length)) >= 0) {
				outf.write(buff, 0, len);
			}
			inf.close();
			outf.close();
		}
		conn = DriverManager.getConnection("jdbc:ucanaccess://" + filename);
		conn.setAutoCommit(true);
	}
}
