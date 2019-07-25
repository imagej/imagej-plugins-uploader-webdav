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

import net.imagej.updater.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * A conditional JUnit test for uploading via WebDAV.
 *
 * <p>This test is only activated iff the following system properties are set:
 * <dl>
 * <dt>webdav.test.url</dt><dd>The URL of the update site</dd>
 * <dt>webdav.test.username</dt><dd>The name of the WebDAV account with write permission to the URL</dd>
 * <dt>webdav.test.password</dt><dd>The password of the WebDAV account with write permission to the URL</dd>
 * </dl></p>
 *
 * <p>Any files in the given directory will be deleted before running the test!</p>
 *
 * @author Johannes Schindelin
 * @author Deborah Schmidt
 */
public class WebDAVUploaderTests extends AbstractUploaderTestBase {

	private WebDAVUploader uploader;
	private String base;

	public WebDAVUploaderTests() {
		super("webdav");
		uploader = new WebDAVUploader();
	}

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Before
	public void setupCredentials() {
		final String username = getProperty("username");
		final String password = getProperty("password");
		base = getProperty("url");
		uploader.setBaseUrl(base);
		uploader.setCredentials(username, password);

	}

	@Test
	public void createDirectory() throws Exception {
		if(uploader.directoryExists("plugin")) {
			uploader.delete("plugin/");
		}
		assertFalse(uploader.directoryExists("plugin"));
		uploader.ensureDirectoryExists("plugin");
		assertTrue(uploader.directoryExists("plugin"));
	}

	@Test
	public void uploadFileInFolder() throws Exception {
		File file = createTestFile();
		uploader.upload(new UploadableFile(file, "plugins/" + file.getName()), null, null);
	}

	@Test
	public void uploadFile() throws Exception {
		File file = createTestFile();
		uploader.upload(new UploadableFile(file, file.getName()), null, null);
	}

	@Test
	public void uploadEmptyFile() throws Exception {
		File file = folder.newFile();
		uploader.upload(new UploadableFile(file, file.getName()), null, null);
	}

	@Test
	public void testLogin() throws IOException {
		FilesCollection files = new FilesCollection(folder.getRoot());
		files.addUpdateSite("test", base, "webdav:" + getProperty("username"), null, 0);
		FilesUploader fUploader = new FilesUploader(files, "test");
		assertTrue(uploader.isAllowed());
	}

	@Test
	public void testDirectoryExists() throws IOException, WebDAVUploader.UnauthenticatedException {
		assertTrue(uploader.directoryExists(""));
	}
	@Test
	public void uploadLargeFiles() throws Exception {
		File file = createTestFile();
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		try {
			raf.setLength(200*1000*1000);
		}
		finally {
			raf.close();
		}
		List<Uploadable> toBeUploaded = new ArrayList<>();
		toBeUploaded.add(new UploadableFile(file, file.getName()));

		uploader.upload(toBeUploaded, new ArrayList<>());
	}

	private File createTestFile() throws IOException {
		File file = folder.newFile("testfile.txt");
		PrintWriter writer = new PrintWriter(file, "UTF-8");
		writer.println("The first line");
		writer.println("The second line");
		writer.close();
		return file;
	}
}
