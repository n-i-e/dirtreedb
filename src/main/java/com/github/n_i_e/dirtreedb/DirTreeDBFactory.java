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

import java.io.IOException;
import java.sql.SQLException;

import com.github.n_i_e.dirtreedb.lister.PathEntryListerFactory;

public class DirTreeDBFactory {

	public static IDirTreeDB getDirTreeDB(String dbfile) throws ClassNotFoundException, SQLException, IOException {
		IDirTreeDB result;
		if (PathEntryListerFactory.fileExtensionMatches(dbfile, "sqlite")) {
			result = new SQLiteDirTreeDB(dbfile);
		} else if (PathEntryListerFactory.fileExtensionMatches(dbfile, "mdb")) {
			result = new MDBDirTreeDB(dbfile);
		} else if (PathEntryListerFactory.fileExtensionMatches(dbfile, "mv.db")) {
			result = new H2DirTreeDB(dbfile);
		} else if (PathEntryListerFactory.fileExtensionMatches(dbfile, "script")) {
			result = new HSQLDBDirTreeDB(dbfile);
		} else {
			result = null;
		}
		return result;
	}

}
