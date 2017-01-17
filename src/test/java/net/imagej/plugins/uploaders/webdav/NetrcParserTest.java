/*
 * #%L
 * ImageJ software for multidimensional image processing and analysis.
 * %%
 * Copyright (C) 2009 - 2017 Board of Regents of the University of
 * Wisconsin-Madison, Broad Institute of MIT and Harvard, and Max Planck
 * Institute of Molecular Cell Biology and Genetics.
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

package net.imagej.plugins.uploaders.webdav;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import net.imagej.plugins.uploaders.webdav.NetrcParser;
import net.imagej.plugins.uploaders.webdav.NetrcParser.Credentials;

import org.junit.Test;

/**
 * Tests the .netrc parser.
 * 
 * @author Johannes Schindelin
 */
public class NetrcParserTest {

	@Test
	public void testParser() throws IOException {
		final NetrcParser parser = new NetrcParser(getClass().getResource("netrc.for-testing"));

		assertNull(parser.getCredentials("no such machine"));

		final Credentials c1 = parser.getCredentials("third");
		assertNotNull(c1);
		assertEquals("login", c1.getUsername());
		assertEquals("x123", c1.getPassword());

		final Credentials c2 = parser.getCredentials("123");
		assertEquals("world", c2.getUsername());
		assertEquals("pass", c2.getPassword());

		final Credentials c3 = parser.getCredentials("123", "second");
		assertEquals("second", c3.getUsername());
		assertEquals("3", c3.getPassword());
	}

	@Test
	public void testWithoutValidFile() throws IOException {
		final NetrcParser parser = new NetrcParser(null);
		assertFalse(parser.getCredentials((Credentials)null));
	}
}
