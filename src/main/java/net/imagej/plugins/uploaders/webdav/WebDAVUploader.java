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
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.jackrabbit.webdav.DavConstants;
import org.apache.jackrabbit.webdav.client.methods.*;
import org.apache.jackrabbit.webdav.lock.LockInfo;
import org.apache.jackrabbit.webdav.lock.Scope;
import org.apache.jackrabbit.webdav.lock.Type;
import org.scijava.log.LogLevel;
import org.scijava.log.LogService;
import org.scijava.log.StderrLogService;
import org.scijava.plugin.Plugin;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

/**
 * Uploads files to an update server using WebDAV.
 * 
 * @author Johannes Schindelin
 * @author Deborah Schmidt
 */
@Plugin(type = Uploader.class)
public class WebDAVUploader extends AbstractUploader {

	private String baseURL,username, password;
	private final Set<String> existingDirectories;
	private LogService log;
	private boolean debug = false;
	protected static HttpClient client;
	private CredentialsProvider provider;
	ArrayList<String> schemes = new ArrayList<>();

	static class UnauthenticatedException extends Exception {}

	public WebDAVUploader() {
		provider = new BasicCredentialsProvider();
		int timeout = 360;
		schemes.add(AuthSchemes.DIGEST);
		schemes.add(AuthSchemes.BASIC);
		RequestConfig config = RequestConfig.custom()
				.setConnectTimeout(timeout * 1000)
				.setConnectionRequestTimeout(timeout * 1000)
				.setSocketTimeout(timeout * 1000)
				.build();
		client = HttpClientBuilder.create()
				.setDefaultCredentialsProvider(provider)
				.setDefaultRequestConfig(config)
				.build();

		existingDirectories = new HashSet<>();
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
		setBaseUrl(site.getURL());

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

		setCredentials(username, password);

		try {
			if (!isAllowed()) {
				UpdaterUserInterface.get().error("User " + username + " lacks upload permissions for " + baseURL + " or the password is incorrect.");
				return false;
			}
			if (!directoryExists("")) {
				UpdaterUserInterface.get().error(baseURL + " does not exist yet!");
				return false;
			}
		}
		catch (UnauthenticatedException e) {
			UpdaterUserInterface.get().error("User " + username + " lacks upload permissions for " + baseURL + " or the password is incorrect.");
			return false;
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public void logout() {
		username = password = null;
	}

	// Steps to accomplish entire upload task
	@Override
	public synchronized void upload(final List<Uploadable> sources,
		final List<String> locks) throws IOException {
		timestamp = -1;
		Map<String, String> tokens = new HashMap<>();
		for (final String lock : locks) {
			final String path = lock + ".lock";
			final String token = lock(path);
			tokens.put(path, token);
		}
		setTitle("Uploading");
		calculateTotalSize(sources);
		int count = 0;
		try {
			for (final Uploadable source : sources) {

				final String target = source.getFilename();
				String token = tokens.get(target);

				// make sure that the target directory exists
				int slash = target.lastIndexOf('/');
				if (slash > 0 && ! ensureDirectoryExists(target.substring(0, slash + 1))) {
					throw new IOException("Could not make subdirectory for " + target);
				}

				addItem(source);
				final int[] currentCount = {0};
				final int currentTotal = (int) source.getFilesize();
				int finalCount = count;

				ProgressHttpEntityWrapper.ProgressCallback progressCallback = progress -> {
					currentCount[0] = (int) (currentTotal * progress);
					setItemCount(currentCount[0], currentTotal);
					setCount(finalCount + currentCount[0], total);
				};

				upload(source, token, progressCallback);

				itemDone(source);
				count += currentCount[0];
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
		} finally {
			for (final String key : tokens.keySet()) {
				final String token = tokens.get(key);
				if (!unlock(key, token)) {
					throw new IOException("Could not unlock " + key + " with token " + token);
				}
			}
		}
	}

	boolean upload(Uploadable source, String token, ProgressHttpEntityWrapper.ProgressCallback progressCallback) throws IOException {

		String target = source.getFilename();
		URL url = getURL(target, false);
		HttpPut method = new HttpPut(url.toString());
		if(token != null) {
			method.setHeader("If", "<" + url + "> (<" + token + ">)");
		}

		HttpEntity entity;
		if(source.getFilesize() > 0) {
			entity = new InputStreamEntity(source.getInputStream(), source.getFilesize());
			((InputStreamEntity)entity).setChunked(true);
		} else {
			entity = new BufferedHttpEntity(new InputStreamEntity(source.getInputStream()));
		}

		if(progressCallback != null) {
			method.setEntity(new ProgressHttpEntityWrapper(entity, progressCallback, source.getFilesize()));
		} else {
			method.setEntity(entity);
		}

		try {
			HttpResponse response = runMethodOnClient(method, createStreamingUploadContext());
			int code = response.getStatusLine().getStatusCode();
			if (code != 201 && code != 204) {
				log.error("Code: " + code + " " + response.getStatusLine());
				throw new IOException("Could not write " + target);
			} else {
				log.info("Successfully uploaded to " + target + "");
				return true;
			}
		} finally {
			method.releaseConnection();
		}
	}

	private HttpClientContext createStreamingUploadContext() {
		final HttpClientContext context = createContext();
		RequestConfig config = RequestConfig.custom().setExpectContinueEnabled(true).build();
		context.setRequestConfig(config);
		return context;
	}

	String lock(final String path) throws IOException {
		HttpLock method = new HttpLock(getURL(path, false).toString(),
				new LockInfo(Scope.EXCLUSIVE, Type.WRITE, username, 600*1000, false));
		boolean success;
		HttpResponse response;
		try {
			response = runMethodOnClient(method);
			success = method.succeeded(response);
		} finally {
			method.releaseConnection();
		}

		if (!success) System.err.println("Error obtaining lock for " + path + ": " + response.getStatusLine());
		else {
			log.info("Successfully locked " + path + ".");
		}
		if (timestamp < 0) {
			Date date = DateUtils.parseDate(response.getFirstHeader("Date").getValue());
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			timestamp = Long.parseLong(UpdaterUtil.timestamp(cal));
			if (timestamp < 0) {
				throw new IOException("Could not obtain date from the server");
			}
		}

		final String token = method.getLockToken(response);
		if (debug) {
			log.info("Tried to obtain a lock (" + token + "):\n" + response.getEntity().getContent().toString());
		}
		if (token == null) {
			log.error("Expected lock for '" + path + "', got:\n" + response.getEntity().getContent().toString());
			throw new IOException("Could not obtain lock for " + path);
		}
		return token;
	}

	boolean unlock(final String path, final String token) throws IOException {
		HttpUnlock method = new HttpUnlock(getURL(path, false).toString(), token);
		boolean success;
		try {
			HttpResponse response = runMethodOnClient(method);
			if (success = method.succeeded(response)) {
				log.info("Successfully unlocked " + path + ".");
			}
			else {
				System.err.println("Error removing lock from " + path + ": " + response.getStatusLine());
			}
		} finally {
			method.releaseConnection();
		}
		return success;
	}

	boolean move(final String source, final String target, final String token, boolean force) throws IOException {
		final String url = getURL(source, false).toString();
		final String targetURL = getURL(target, false).toString();
		HttpMove method = new HttpMove(url, targetURL, force);
		if(token != null)
			method.setHeader("If", "<" + url + "> (<" + token + ">)");
		boolean success = false;
		try {
			HttpResponse response = runMethodOnClient(method);
			success = method.succeeded(response);
			if (success) {
				log.info("Successfully moved  " + source + " to " + target + ".");
			} else {
				log.error("Error moving " + source + " to " + target + ": " + response.getStatusLine());
			}
		} catch (Exception e) {
			log.error(e);
		} finally {
			method.releaseConnection();
		}
		return success;
	}

	boolean ensureDirectoryExists(final String path) throws IOException {
		if (existingDirectories.contains(path)) {
			return true;
		}
		try {
			if (directoryExists(path)) {
				existingDirectories.add(path);
				return true;
			}
		} catch (UnauthenticatedException e) {
			log.error("Could not check if directory " + path + " exists. The given user is unauthorized or the given password is incorrect.");
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

	boolean directoryExists(final String path) throws IOException, UnauthenticatedException {
		HttpPropfind method = new HttpPropfind(getURL(path, true).toString(),
				DavConstants.PROPFIND_ALL_PROP, 0);
		try {
			HttpResponse response = runMethodOnClient(method);
			if(response.getStatusLine().getStatusCode() == 401) {
				log.error("Could not check if directory " + path + " exists. The given user is unauthorized or the given password is incorrect.");
				throw new UnauthenticatedException();
			}
			boolean success = method.succeeded(response);
			if(success) {
				log.info("Successfully called PropFind, directory exists: " + path + ".");
				return true;
			}
		} finally {
			method.releaseConnection();
		}
		return false;
	}

	boolean isAllowed() throws IOException {
		HttpOptions method = new HttpOptions(baseURL);
		boolean success;
		try {
			HttpResponse response = runMethodOnClient(method);
			Header header = response.getFirstHeader("Allow");
			if (header == null) {
				success = false;
//				log.error("Failed to retrieve OPTIONS for WebDAV actions");
			} else {
				success = true;
				log.info("Successfully retrieved OPTIONS.");
				final String allow = header.getValue();
				if (!allow.contains("LOCK")) {
					log.error("LOCK action not allowed; valid actions: " + allow);
					success = false;
				}
			}
		} finally {
			method.releaseConnection();
		}
		return success;
	}

	boolean makeDirectory(final String path) throws IOException {
		HttpMkcol method = new HttpMkcol(getURL(path, true).toString());
		boolean success;
		try {
			HttpResponse response = runMethodOnClient(method);
			success = method.succeeded(response);
		} finally {
			method.releaseConnection();
		}
		if(success) {
			log.info("Successfully made directory " + path + ".");
			return true;
		} else {
			log.error("Failed to make directory " + path + ".");
			return false;
		}
	}

	void delete(final String path) throws IOException {
		final boolean isDirectory = path.endsWith("/");
		final URL target = getURL(path, isDirectory);
		HttpDelete method = new HttpDelete(target.toString());
		if(!isDirectory)
			method.setHeader("Depth", "Infinity");
		try {
			HttpResponse response = runMethodOnClient(method);
			if (method.succeeded(response)) {
				log.info("Successfully deleted " + target + ".");
			}else {
				throw new IOException("Could not delete " + target + ": " + response.getStatusLine());
			}
		} finally {
			method.releaseConnection();
		}
	}

	boolean isDeleted(String path) throws IOException {
		final boolean isDirectory = path.endsWith("/");
		final URL target = getURL(path, isDirectory);
		HttpGet method = new HttpGet(target.toString());
		if(!isDirectory) {
			method.setHeader("Depth", "Infinity");
		}
		boolean success;
		try {
			HttpResponse response = runMethodOnClient(method);
			success = response.getStatusLine().getStatusCode() == 404;
		} finally {
			method.releaseConnection();
		}
		return success;
	}

	private HttpClientContext createContext() {
		final HttpClientContext context = HttpClientContext.create();
		context.setCredentialsProvider(provider);
		RequestConfig config = RequestConfig.custom().setExpectContinueEnabled(true).build();
		context.setRequestConfig(config);
		return context;
	}

	HttpResponse runMethodOnClient(HttpUriRequest method) throws IOException {
		return runMethodOnClient(method, createContext());
	}

	HttpResponse runMethodOnClient(HttpUriRequest method, HttpContext context) throws IOException {
		method.setHeader("User-Agent", "Java/" + System.getProperty("java.version"));
		if (debug) {
			log.debug("Sending request " + method);
			for (Header header : method.getAllHeaders()) {
				log.debug("Header: " + header.getName() + " = " + header.getValue());
			}
		}
		HttpResponse response = client.execute(method, context);
		if (debug) {
			log.debug("Response: " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine());
			for (Header header : response.getAllHeaders()) {
				log.debug("Header: " + header.getName() + " = " + header.getValue());
			}
		}
		return response;
	}

	private URL getURL(final String path, boolean isDirectory) throws MalformedURLException, UnsupportedEncodingException {
		final String url = baseURL + URLEncoder.encode(path, "UTF-8").replaceAll("%2F", "/").replaceAll("\\+","%20");
		if (!isDirectory || "".equals(path) || path.endsWith("/")) return new URL(url);
		return new URL(url + "/");
	}

	void setCredentials(final String username, final String password) {
		this.username = username;
		this.password = password;
		if (log == null) {
			log = new StderrLogService();
			log.setLevel(LogLevel.DEBUG);
			debug = true;
		}
		provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
	}

	void setBaseUrl(String url) {
		baseURL = url;
		if (!baseURL.endsWith("/")) baseURL += "/";
	}

}
