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
import java.io.InputStream;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

public class ApacheCompressCompressorLister extends AbstractCompressorLister {
	public ApacheCompressCompressorLister (PathEntry basepath, InputStream inf) throws IOException {
		super(basepath, inf);
		try {
			setInstream(new CompressorStreamFactory().createCompressorInputStream(inf));
		} catch (CompressorException e) {
			throw new IOException(e.toString());
		}
	}
}
