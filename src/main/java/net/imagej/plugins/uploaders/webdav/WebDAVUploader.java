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

import net.imagej.plugins.uploaders.webdav.NetrcParser.Credentials;
import net.imagej.updater.*;
import net.imagej.updater.util.UpdaterUserInterface;
import net.imagej.updater.util.UpdaterUtil;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.jackrabbit.webdav.client.methods.*;
import org.apache.jackrabbit.webdav.lock.Scope;
import org.apache.jackrabbit.webdav.lock.Type;
import org.apache.jackrabbit.webdav.property.DavPropertyNameSet;
import org.scijava.log.LogService;
import org.scijava.log.StderrLogService;
import org.scijava.plugin.Plugin;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

/**
 * Uploads files to an update server using WebDAV.
 * 
 * @author Johannes Schindelin
 */
@Plugin(type = Uploader.class)
public class WebDAVUploader extends AbstractUploader {

	private String baseURL,username, password;
	private Set<String> existingDirectories;
	private LogService log;
	private boolean debug = true;
	protected HttpClient client;

	public WebDAVUploader() {
		client = new HttpClient();
	}

	@Override
	public String getProtocol() {
		return "webdav";
	}

	@Override
	public boolean login(final FilesUploader uploader) {
		if (!super.login(uploader)) return false;

		log = uploader.getLog();
		debug = log.isDebug();

		String host = uploader.getUploadHost();
		if (!"".equals(host)) {
			username = host;
			int colon = username.indexOf(':');
			if (colon < 0) {
				password = null;
			} else {
				password = username.substring(colon + 1);
				username = username.substring(0, colon);
			}
		}

		UpdateSite site = uploader.getFilesCollection().getUpdateSite(uploader.getSiteName(), true);
		baseURL = site.getURL();

		if (username == null || password == null) {
			int colon = baseURL.indexOf("://");
			if (colon > 0) try {
				int slash = baseURL.indexOf('/', colon + 3);
				final String hostname = baseURL.substring(colon + 3, slash < 0 ? baseURL.length() : slash);
				final NetrcParser netrcParser = new NetrcParser();
				final Credentials credentials = netrcParser.getCredentials(hostname, username);
				if (credentials != null) {
					username = credentials.getUsername();
					password = credentials.getPassword();
				}
			} catch (final IOException e) {
				log.warn(e);
			}
		}

		if (username == null) {
			uploader.getDefaultUsername();
			if (username == null) username = UpdaterUserInterface.get().getString("Login for " + baseURL);
			if (username == null) return false;
		}

		if (password == null) {
			final String prompt = "Password for " + username + "@" + baseURL;
			password = UpdaterUserInterface.get().getPassword(prompt);
			if (password == null) return false;
		}

		if (!baseURL.endsWith("/")) baseURL += "/";

		existingDirectories = new HashSet<>();

		setCredentials(username, password);

		if (!isAllowed()) {
			UpdaterUserInterface.get().error("User " + username + " lacks upload permissions for " + baseURL);
			return false;
		}
		try {
			if (!directoryExists("")) {
				UpdaterUserInterface.get().error(baseURL + " does not exist yet!");
				return false;
			}
		} catch (UnauthenticatedException e) {
			return false;
		}
		return true;
	}

	private static class UnauthenticatedException extends Exception {
		private static final long serialVersionUID = 8269335582341674291L;
	}

	@Override
	public void logout() {
		username = password = null;
	}

	// Steps to accomplish entire upload task
	@Override
	public synchronized void upload(final List<Uploadable> sources,
		final List<String> locks) throws IOException
	{
		timestamp = -1;
		Map<String, String> tokens = new HashMap<String, String>();
		for (final String lock : locks) {
			final String path = lock + ".lock";
			final String token = lock(path);
			tokens.put(path, token);
		}
		setTitle("Uploading");

		try {
			calculateTotalSize(sources);
			int count = 0;
			final byte[] buf = new byte[16384];
			for (final Uploadable source : sources) {
				final String target = source.getFilename();

				// make sure that the target directory exists
				int slash = target.lastIndexOf('/');
				if (slash > 0 && ! ensureDirectoryExists(target.substring(0, slash + 1))) {
					throw new IOException("Could not make subdirectory for " + target);
				}

				addItem(source);
				final InputStream input = source.getInputStream();
				int currentCount = 0;
				final int currentTotal = (int) source.getFilesize();
				String token = tokens.get(target);
				URL url = getURL(target, false);

				PutMethod httpMethod = new PutMethod(url.toString());
				if(token != null) {
					httpMethod.setRequestHeader("If", "<" + url + "> (<" + token + ">)");
				}
				httpMethod.setRequestHeader("Content-Length", "" + source.getFilesize());

				CountingInputStream cis = new CountingInputStream(input);

				RequestEntity requestEntity = new InputStreamRequestEntity(cis);
				httpMethod.setRequestEntity(requestEntity);

				Thread thread = new Thread(() -> {
					try {
						runMethodOnClient(httpMethod);
					} catch (HttpException e) {
						System.err.println("Fatal protocol violation: " + e.getMessage());
						e.printStackTrace();
					} catch (IOException e) {
						System.err.println("Fatal transport error: " + e.getMessage());
						e.printStackTrace();
					} finally {
						httpMethod.releaseConnection();
					}

				});

				thread.start();

				for (;;) {
					if (!thread.isAlive()) break;
					currentCount = (int)cis.getByteCount();
					setItemCount(currentCount, currentTotal);
					setCount(count + currentCount, total);
				}
				count += currentCount;
				cis.close();
				itemDone(source);
				int code = httpMethod.getStatusCode();
				if (!httpMethod.succeeded()) {
					log.error("Code: " + code + " " + httpMethod.getStatusLine());
					throw new IOException("Could not write " + target);
				} else {
					log.info("Upload of " + target + " successful.");
				}

			}
			done();

			addItem("Moving locks");
			for (final String lock : locks) {
				final String source = lock + ".lock";
				if (move(source, lock, tokens.get(source), true)) {
					/*
					 * According to RFC4918, a MOVE *must not* move the locks.
					 * And it also says a MOVE is equivalent to a COPY followed
					 * by a DELETE, hence the lock is gone upon a successful
					 * MOVE.
					 */
					tokens.remove(source);
				} else {
					log.error("Could not move " + source + " to " + lock);
				}
			}
		} catch (Exception e) {
			log.error(e);
		} finally {
			for (final String key : tokens.keySet()) {
				final String token = tokens.get(key);
				if (!unlock(key, token)) {
					throw new IOException("Could not unlock " + key + " with token " + token);
				}
			}
		}
	}

	private void runMethodOnClient(HttpMethod httpMethod) throws IOException {
		httpMethod.setRequestHeader("User-Agent", "Java/" + System.getProperty("java.version"));
		if (debug) {
			log.debug("Sending request " + httpMethod.getName() + " " + httpMethod.getURI());
			for (Header header : httpMethod.getRequestHeaders()) {
				log.debug("Header: " + header.getName() + " = " + header.getValue());
			}
		}
		client.executeMethod(httpMethod);
		if (debug) {
			log.debug("Response: " + httpMethod.getStatusCode() + " " + httpMethod.getStatusLine());
			for (Header header : httpMethod.getResponseHeaders()) {
				log.debug("Header: " + header.getName() + " = " + header.getValue());
			}
		}
	}

	private String lock(final String path) throws IOException {
		try {

			LockMethod httpMethod = new LockMethod(getURL(path, false).toString(),
					Scope.EXCLUSIVE, Type.WRITE, username, (long)(600*60), false);
			//TODO test if one can remove these headers
			httpMethod.setRequestHeader("Timeout", "Second-600");
			httpMethod.setRequestHeader("Brief", "t");
			httpMethod.setRequestHeader("Content-Type", "text/xml; charset=\"utf-8\"");

			runMethodOnClient(httpMethod);

			if (!httpMethod.succeeded()) System.err.println("Error obtaining lock for " + path + ": " + httpMethod.getStatusLine());
			else {
				log.info("Successfully locked " + path + ".");
			}
			if (timestamp < 0) {
				Date date = DateUtil.parseDate(httpMethod.getResponseHeader("Date").getValue());
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				timestamp = Long.parseLong(UpdaterUtil.timestamp(cal));
				if (timestamp < 0) {
					throw new IOException("Could not obtain date from the server");
				}
			}

			final String token = httpMethod.getLockToken();
			if (debug) {
				log.info("Tried to obtain a lock (" + token + "):\n" + httpMethod.getResponseBodyAsString());
			}
			if (token == null) {
				log.error("Expected lock for '" + path + "', got:\n" + httpMethod.getResponseBodyAsString());
				throw new IOException("Could not obtain lock for " + path);
			}
			return token;
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			log.error(e);
			throw new RuntimeException(e);
		}
	}

	private boolean unlock(final String path, final String token) {
		try {
			UnLockMethod httpMethod = new UnLockMethod(getURL(path, false).toString(), token);
			httpMethod.setRequestHeader("Content-Type", "application/octet-stream");
			client.executeMethod(httpMethod);

			if (!httpMethod.succeeded()) System.err.println("Error removing lock from " + path + ": " + httpMethod.getStatusLine());
			else {
				log.info("Successfully unlocked " + path + ".");
			}
		} catch (Exception e) {
			log.error(e);
		}
		return false;
	}

	private boolean move(final String source, final String target, final String token, boolean force) {
		try {
			final String url = getURL(source, false).toString();
			final String targetURL = getURL(target, false).toString();
			MoveMethod httpMethod = new MoveMethod(url, targetURL, force);
			httpMethod.setRequestHeader("Content-Type", "application/octet-stream");
			if(token != null)
				httpMethod.setRequestHeader("If", "<" + url + "> (<" + token + ">)");

			runMethodOnClient(httpMethod);

			if (httpMethod.succeeded()) {
				log.info("Successfully moved  " + source + " to " + target + ".");
				return true;
			}
			log.error("Error moving " + source + " to " + target + ": " + httpMethod.getStatusLine());
		} catch (Exception e) {
			log.error(e);
		}
		return false;
	}

	private boolean ensureDirectoryExists(final String path) {
		if (existingDirectories.contains(path)) {
			return true;
		}

		try {
			if (directoryExists(path)) {
				existingDirectories.add(path);
				return true;
			}
		} catch (UnauthenticatedException e) {
			return false;
		}

		int slash = path.lastIndexOf('/', path.length() - 2);
		if (slash > 0 && !ensureDirectoryExists(path.substring(0, slash + 1))) {
			return false;
		}

		if (makeDirectory(path)) {
			existingDirectories.add(path);
			return true;
		}

		return false;
	}

	private boolean directoryExists(final String path) throws UnauthenticatedException {
		try {
			PropFindMethod httpMethod = new PropFindMethod(getURL(path, false).toString(),
					PropFindMethod.PROPFIND_PROPERTY_NAMES, new DavPropertyNameSet(), 0);

			httpMethod.setRequestHeader("Timeout", "Second-600");
			httpMethod.setRequestHeader("Brief", "t");
			httpMethod.setRequestHeader("Content-Type", "text/xml; charset=\"utf-8\"");
			runMethodOnClient(httpMethod);

			int code = httpMethod.getStatusCode();
			if(httpMethod.succeeded()) {
				log.info("Successfully called PropFind on " + path + ".");
			}
			if (code == 401) throw new UnauthenticatedException();

			return httpMethod.succeeded();
		} catch (FileNotFoundException e) {
			return false;
		} catch (IOException e) {
			log.error(e);
		}
		return false;
	}

	private boolean isAllowed() {
		try {
			OptionsMethod httpMethod = new OptionsMethod(baseURL);
//			httpMethod.setRequestHeader("Content-Type", "application/octet-stream");
			runMethodOnClient(httpMethod);
			if(httpMethod.succeeded()) {
				log.info("Successfully retrieved OPTIONS.");
			}
			final String allow = httpMethod.getResponseHeader("Allow").getValue();
			if (allow == null) {
				log.error("Failed to retrieve OPTIONS for WebDAV actions");
				return false;
			}
			if (!allow.contains("LOCK")) {
				log.error("LOCK action not allowed; valid actions: " + allow);
				return false;
			}
			return true;
		}
		catch (final Exception e) {
			log.error(e);
		}
		return false;
	}

	private boolean makeDirectory(final String path) {
		try {
			MkColMethod httpMethod = new MkColMethod(getURL(path, true).toString());
			httpMethod.setRequestHeader("Content-Type", "application/octet-stream");
			runMethodOnClient(httpMethod);
			if(httpMethod.succeeded()) {
				log.info("Successfully made directory " + path + ".");
			}
			return httpMethod.succeeded();
		} catch (Exception e) {
			log.error(e);
		}
		return false;
	}

	private URL getURL(final String path, boolean isDirectory) throws MalformedURLException, UnsupportedEncodingException {
		final String url = baseURL + URLEncoder.encode(path, "UTF-8").replaceAll("%2F", "/").replaceAll("\\+","%20");
		if (!isDirectory || "".equals(path) && path.endsWith("/")) return new URL(url);
		return new URL(url + "/");
	}

	protected void setCredentials(final String username, final String password) {
		this.username = username;
		this.password = password;
		if (log == null) {
			log = new StderrLogService();
			log.setLevel(LogService.DEBUG);
			debug = true;
		}

		client.getState().setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
	}

}
