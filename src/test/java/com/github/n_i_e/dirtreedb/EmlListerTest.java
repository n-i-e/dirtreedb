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

import junit.framework.TestCase;

/**
 * @author Namihiko
 *
 */
public class EmlListerTest extends TestCase {

	String message1 = "Subject: test\n\ntest\n";

	public EmlListerTest(String name) {
		super(name);
	}

	public void testEmlLister() throws IOException {
		InputStream inf = new ByteArrayInputStream(message1.getBytes());
		PathEntry p0 = new PathEntry("test.eml", PathEntry.FILE);
		EmlLister t = new EmlLister(p0, inf);
		assertTrue(t.hasNext());
		PathEntry p = t.next();
		assertEquals("test.eml/Subject: test.txt", p.getPath());
		assertFalse(t.hasNext());
		t.close();
	}

}
