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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.poi.hsmf.MAPIMessage;
import org.apache.poi.hsmf.datatypes.AttachmentChunks;
import org.apache.poi.hsmf.exceptions.ChunkNotFoundException;

public class MsgLister extends AbstractArchiveLister {
	private int count = 0;
	private AttachmentChunks[] content = null;
	private long date = 0;
	private String subject = null;
	private InputStream inf, instream = null;

	public MsgLister (PathEntry basepath, InputStream inf) throws IOException {
		super(basepath);
		Assertion.assertNullPointerException(inf != null);
		this.inf = inf;
	}

	@Override
	public InputStream getInputStream() {
		Assertion.assertNullPointerException(instream != null);
		return instream;
	}

	@Override
	protected PathEntry getNext() throws IOException {
		try {
			if (content == null) {
				MAPIMessage msg;
				msg = new MAPIMessage(inf);

				date = msg.getMessageDate() == null ? 0L : msg.getMessageDate().getTimeInMillis();
				subject = msg.getSubject();

				String s = subjectToFilename(subject, "text/plain", 1);
				s = s.replace("\\", "/");
				Assertion.assertAssertionError(!s.equals(""));
				PathEntry next_entry = new PathEntry(getBasePath().getPath() + "/" + s, PathEntry.COMPRESSEDFILE);
				next_entry.setDateLastModified(date);
				next_entry.setStatus(PathEntry.DIRTY);
				byte[] body = getByteArrayWithBom(msg.getTextBody());
				next_entry.setCompressedSize(body.length);
				next_entry.setSize(next_entry.getSize());

				instream = new ByteArrayInputStreamWithCascadingClose(body);
				if (isCsumRequested()) {
					next_entry.setCsumAndClose(instream);
				}
				content = msg.getAttachmentFiles();
				return next_entry;
			} else {
				AttachmentChunks part = null;
				byte[] data = null;
				while (data == null) {
					if (count >= content.length) {
						return null;
					}
					part = content[count];
					try {
						data = part.attachData.getValue();
					} catch (NullPointerException e) {
						data = null;
						count++;
					}
				}

				String filename;
				try {
					filename = part.attachFileName.getValue();
				} catch (NullPointerException e) {
					filename = String.valueOf(count);
				}
				filename = filename.replace("\\", "/");
				if (filename.equals("")) {
					filename = String.valueOf(count);
				}

				PathEntry next_entry = new PathEntry(getBasePath().getPath() + "/" + filename, PathEntry.COMPRESSEDFILE);
				next_entry.setDateLastModified(date);
				next_entry.setStatus(PathEntry.DIRTY);
				next_entry.setCompressedSize(data.length);
				next_entry.setSize(data.length);

				instream = new ByteArrayInputStreamWithCascadingClose(data);
				if (isCsumRequested()) {
					next_entry.setCsumAndClose(instream);
				}
				count++;
				return next_entry;
			}
		} catch (ChunkNotFoundException e) {
			return null;
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		inf.close();
	}

	private static String subjectToFilename(String subject, String type, int subjectFilenameCount) {
		String ext;
		if (type.startsWith("text/html")) {
			ext = ".html";
		} else if (type.startsWith("text/plain")) {
			ext = ".txt";
		} else {
			ext = "";
		}
    	String filename;
		if (subjectFilenameCount == 1) {
			filename = "Subject: " + subject + ext;
		} else {
			filename = "Subject: " + subject + " (" + subjectFilenameCount + ")" + ext;
		}
		return filename;
	}

	private static byte[] getByteArrayWithBom(String content) {
		try {
			byte[] contentEncoded = content.getBytes("UTF-8");
			byte[] result = new byte[contentEncoded.length + 3];
			// UTF-8 BOM
			result[0] = (byte)0xEF;
			result[1] = (byte)0xBB;
			result[2] = (byte)0xBF;
			System.arraycopy(contentEncoded, 0, result, 3, contentEncoded.length);
			return result;
		} catch (UnsupportedEncodingException e) {
			return new byte[0];
		}
	}

	private class ByteArrayInputStreamWithCascadingClose extends ByteArrayInputStream {

		public ByteArrayInputStreamWithCascadingClose(byte[] inArray) {
			super(inArray);
		}

		@Override
		public void close() throws IOException {
			super.close();
			MsgLister.this.close();
		}
	}
}
