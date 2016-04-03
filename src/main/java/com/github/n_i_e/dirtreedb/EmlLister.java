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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.UnsupportedEncodingException;

import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeUtility;

public class EmlLister extends AbstractArchiveLister {
	int count = 0;
	boolean eof = false;
	int subjectFilenameCount = 1;
	Multipart content = null;
	long date = 0;
	String subject = null;
	InputStream inf, instream = null;

	public EmlLister (PathEntry basepath, InputStream inf) throws IOException {
		super(basepath);
		Assertion.assertNullPointerException(inf != null);
		this.inf = inf;
	}

	public InputStream getInputStream() {
		Assertion.assertNullPointerException(instream != null);
		return instream;
	}

	protected void getNext(boolean csum) throws IOException {
		if (next_entry != null) {
			return;
		} else if (eof) {
			return;
		}
		try {
			if (content == null) {
				Message msg = new MimeMessage(null, inf);

				date = msg.getSentDate() == null ? 0L : msg.getSentDate().getTime();
				subject = msg.getSubject();
				String msgtype = getNormalizedContentType(msg.getContentType());

				if (msgtype.startsWith("multipart/")) {
					content = (Multipart) msg.getContent();
				} else {
					String s = subjectToFilename(subject, msgtype, 1);
					s = s.replace("\\", "/");
					next_entry = new PathEntry(basepath.getPath() + "/" + s, PathEntry.COMPRESSEDFILE);
					next_entry.setDateLastModified(date);
					next_entry.setStatus(PathEntry.DIRTY);
					next_entry.setCompressedSize(msg.getSize());
					next_entry.setSize(next_entry.getSize());

					if (msgtype.startsWith("text/plain")) {
						instream = textContentToInputStream(msg.getContent());
					} else {
						instream = binaryContentToInputStream(msg.getContent());
					}
					if (csum) {
						next_entry.setCsum(instream);
					}
					eof = true;
					return;
				}
			}

			BodyPart part = content.getBodyPart(count);
			String type = getNormalizedContentType(part.getContentType());
			String filename = part.getFileName();
			if (filename == null) {
				filename = subjectToFilename(subject, type, subjectFilenameCount++);
			} else if (filename.startsWith("=?")) {
				filename = MimeUtility.decodeText(filename);
			}
			filename = filename.replace("\\", "/");
			next_entry = new PathEntry(basepath.getPath() + "/" + filename, PathEntry.COMPRESSEDFILE);
			next_entry.setDateLastModified(date);
			next_entry.setStatus(PathEntry.DIRTY);
			next_entry.setCompressedSize(part.getSize());
			next_entry.setSize(next_entry.getSize());

			if (type.startsWith("text/plain")) {
				instream = textContentToInputStream(part.getContent());
			} else {
				instream = binaryContentToInputStream(part.getContent());
			}
			if (csum) {
				next_entry.setCsum(instream);
			}

			if (++count >= content.getCount()) {
				eof = true;
			}
		} catch (MessagingException e) {
			eof = true;
		}
		return;
	}

	@Override
	public void close() throws IOException {
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

	private static String getNormalizedContentType(String contentType) {
    	if (contentType == null) {
    		return "text/plain";
    	} else {
    		return contentType.toLowerCase();
    	}
	}

	private InputStream textContentToInputStream(Object content) throws IOException {
		try {
			return new AddBomInputStreamWithCascadingClose((InputStream) content);
		} catch (ClassCastException e) {
			return new ByteArrayInputStreamWithCascadingClose(getByteArrayWithBom(content.toString()));
		}
	}

	private InputStream binaryContentToInputStream(Object content) {
		try {
			return (InputStream) content;
		} catch (ClassCastException e) {
			try {
				return new ByteArrayInputStreamWithCascadingClose((byte[]) content);
			} catch (ClassCastException e2) {
				return new ByteArrayInputStreamWithCascadingClose(getByteArrayWithBom(content.toString()));
			}
		}
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
			EmlLister.this.close();
		}
	}

	private class AddBomInputStreamWithCascadingClose extends AddBomInputStream {

		public AddBomInputStreamWithCascadingClose(InputStream inf) throws IOException {
			super(inf);
		}

		@Override
		public void close() throws IOException {
			super.close();
			EmlLister.this.close();
		}
	}

	private static class AddBomInputStream extends SequenceInputStream {

		static final byte[] utf8bom = {(byte)0xEF, (byte)0xBB, (byte)0xBF};

		public AddBomInputStream(InputStream inf) throws IOException {
			super(new ByteArrayInputStream(utf8bom), eliminateBom(inf));
		}

		private static InputStream eliminateBom(InputStream inf) throws IOException {
			if (!inf.markSupported()) {
				inf = new BufferedInputStream(inf);
			}
			inf.mark(3);
			if (inf.available() >= 3) {
				byte[] buf = new byte[3];
				inf.read(buf, 0, 3);
				if (buf[0] != 0xEF || buf[1] != 0xBB || buf[2] != 0xBF) {
					inf.reset();
				}
			}
			return inf;
		}
	}
}
