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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class GetCompressedFileSizeException extends Exception {
	GetCompressedFileSizeException(String s) {
		super(s);
	}
}

/**
 * Represents a file or folder in the filesystem, or a file entry in a zip/tar/... archive.
 * Used for DirTreeDB.
 */
public class PathEntry {
	/**
	 * datelastmodified in Java Epoch (milliseconds since 1970-01-01 00:00:00 GMT)
	 */
	private long datelastmodified=0;
	private long size=0;
	private long compressedsize=0;
	private int csum=0; // not long but int, for compatibility with MDB
	private boolean csumIsNull=true;
	private String path=null;
	/**
	 * 0 = folder, 1 = file, 2 = folder in archive, 3 = file in archive.
	 */
	private int type=-1;

	public static final int UNKNOWN=-1;
	public static final int FOLDER=0;
	public static final int FILE=1;
	public static final int COMPRESSEDFOLDER=2;
	public static final int COMPRESSEDFILE=3;

	/**
	 * 0 = clean (no need to run DirTreeDB.list()), 1 = dirty (need to run DirTreeDB.list())
	 */
	private int status=0;

	public static final int CLEAN=0;
	public static final int DIRTY=1;
	public static final int NOACCESS=2;

	public PathEntry() {
		// noop
	}

	public PathEntry(File dh) throws IOException {
		datelastmodified = (dh.lastModified()/1000)*1000; // sub-second is eliminated
		if (dh.isDirectory()) {
			path = dh.getCanonicalPath();
			if (! "\\".equals(path.substring(path.length()-1))) {
				this.path += "\\";
			}
			type = FOLDER;
		} else {
			path = dh.getCanonicalPath();
			size = dh.length();
			try {
				this.compressedsize = win32_GetCompressedSize(path);
			} catch (GetCompressedFileSizeException e) {
				this.compressedsize = size;
			}
			type = FILE;
		}
		if (dh.canRead()) {
			status = DIRTY;
		} else {
			status = NOACCESS;
		}
		csumIsNull = true;

		assertInvariantConditions();
	}

	public PathEntry(PathEntry oldentry) {
		this.datelastmodified = oldentry.datelastmodified;
		this.size = oldentry.size;
		this.compressedsize = oldentry.compressedsize;
		this.csum = oldentry.csum;
		this.csumIsNull = oldentry.csumIsNull;
		this.path = oldentry.path;
		this.type = oldentry.type;
		this.status = oldentry.status;

		assertInvariantConditions();
	}

	public PathEntry(String path, int type) {
		this.datelastmodified = 0;
		this.size = 0;
		this.compressedsize = 0;
		this.csum = 0;
		this.csumIsNull = true;
		this.path = path;
		this.type = type;
		this.status = PathEntry.DIRTY;

		assertInvariantConditions();
	}

	public long getDateLastModified() {
		return datelastmodified;
	}

	public void setDateLastModified(long date) {
		datelastmodified = (date/1000)*1000;
		assert(datelastmodified % 1000 == 0);
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public long getCompressedSize() {
		return compressedsize;
	}

	public void setCompressedSize(long compressedsize) {
		this.compressedsize = compressedsize;
	}

	public String getPath() {
		return path;
	}

	public int getType() {
		return type;
	}

	public boolean isFolder() {
		return getType() == PathEntry.FOLDER ? true : false;
	}

	public boolean isFile() {
		return getType() == PathEntry.FILE ? true : false;
	}

	public boolean isCompressedFile() {
		return getType() == PathEntry.COMPRESSEDFILE ? true : false;
	}

	public boolean isCompressedFolder() {
		return getType() == PathEntry.COMPRESSEDFOLDER ? true : false;
	}

	public int getStatus() {
		return status;
	}

	public boolean isClean() {
		return getStatus() == PathEntry.CLEAN? true : false;
	}

	public boolean isDirty() {
		return getStatus() == PathEntry.DIRTY? true : false;
	}

	public boolean isNoAccess() {
		return getStatus() == PathEntry.NOACCESS? true : false;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getCsum() {
		if (csumIsNull) {
			throw new NullPointerException("!! csum is null for: " + getPath());
		} else {
			return csum;
		}
	}

	public void setCsum(int csum) {
		this.csum = csum;
		csumIsNull = false;
	}

	public void setCsum(InputStream inf) throws IOException {
		try {
			int contentsize = 0;
			MessageDigest md5digest = MessageDigest.getInstance("MD5");

			byte[] buff = new byte[4096];
			int len;
			while ((len = inf.read(buff, 0, buff.length)) >= 0) {
				md5digest.update(buff, 0, len);
				contentsize += len;
			}

			byte[] md5all = md5digest.digest();
			csum = ByteBuffer.wrap(md5all).getInt();
			csumIsNull = false;
			size = contentsize;
		} catch (NoSuchAlgorithmException e) {
			csumIsNull = true;
		} catch (IOException e) {
			csumIsNull = true;
			throw e;
		}
	}

	public void setCsumAndClose(InputStream inf) throws IOException {
		try {
			setCsum(inf);
		} finally {
			inf.close();
		}
	}

	public InputStream getInputStream() throws IOException {
		Assertion.assertAssertionError(isFile() || isCompressedFile());
		if (isFile()) {
			return new BufferedInputStream(new FileInputStream(new File(getPath())), 1*1024*1024);
		} else {
			int n1 = getPath().indexOf("/");
			Assertion.assertAssertionError(n1 >= 0);
			File f1 = new File(getPath().substring(0, n1));
			Assertion.assertFileNotFoundException(f1.exists(), "!! File not found: " + f1.getPath());
			PathEntry parent = new PathEntry(f1);
			InputStream parentStream = parent.getInputStream();
			while (true) {
				IArchiveLister lister = ArchiveListerFactory.getArchiveLister(parent, parentStream);
				while (lister.hasNext()) {
					PathEntry p = lister.next();
					if (p.getPath().equals(getPath())) {
						return new BufferedInputStream(lister.getInputStream());
					} else if (getPath().startsWith(p.getPath())) {
						parent = p;
						parentStream = lister.getInputStream();
						break;
					}
					throw new FileNotFoundException("!! Archive file not found for path: " + getPath());
				}
			}

		}
	}

	public void clearCsum() {
		csumIsNull = true;
	}

	public boolean isCsumNull() {
		return csumIsNull;
	}

	private static synchronized long win32_GetCompressedSize(String path) throws GetCompressedFileSizeException
	{
		Kernel32 kernel32 = Kernel32.INSTANCE;
		int size_high[] = {0};
		int size_low = kernel32.GetCompressedFileSizeA(path, size_high);
		int error = kernel32.GetLastError();
		if (error != 0) {
			throw new GetCompressedFileSizeException(String.format("!! GetCompressedFileSize Error; code %d, path %s", error, path));
		}
		ByteBuffer buff = ByteBuffer.allocate(8);
		buff.clear();
		buff.putInt(size_high[0]);
		buff.putInt(size_low);
		buff.flip();
		long result = buff.getLong();
		
		return result;
	}

	private void assertInvariantConditions() {
		assert(!isFolder()           ||  getPath().endsWith("\\")) : "Folder path is " + getPath();
		assert(!isFolder()           || !getPath().contains("/"))  : "Folder path is " + getPath();
		assert(!isFolder()           ||  isCsumNull())             : "Folder path is " + getPath();

		assert(!isFile()             || !getPath().endsWith("\\")) : "File path is " + getPath();
		assert(!isFile()             || !getPath().contains("/"))  : "File path is " + getPath();

		assert(!isCompressedFolder() ||  getPath().endsWith("/"))  : "CompressedFolder path is " + getPath();
		assert(!isCompressedFolder() ||  isCsumNull())             : "CompressedFolder path is " + getPath();

		assert(!isCompressedFile()   || !getPath().endsWith("/"))  : "CompressedFile path is " + getPath();
		assert(!isCompressedFile()   ||  getPath().contains("/"))  : "CompressedFile path is " + getPath();
	}

}
