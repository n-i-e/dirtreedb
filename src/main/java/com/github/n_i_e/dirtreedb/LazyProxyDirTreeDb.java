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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LazyProxyDirTreeDb extends ProxyDirTreeDbWithUpdateQueue {

	private static int numCrawlingThreads = 1;

	static {
		PreferenceRW.regist(new IPreferenceSyncUpdate() {
			@Override public void setDbFilePath(String dbFilePath) {}
			@Override public void setExtensionAvailabilityMap(Map<String, Boolean> extensionAvailabilityMap) {}
			@Override public void setNumCrawlingThreads(int numCrawlingThreads) {
				LazyProxyDirTreeDb.setNumCrawlingThreads(numCrawlingThreads);
			}
			@Override public void setWindowsIdleSeconds(int windowsIdleSeconds) {}
			@Override public void setCharset(String newvalue) {}
		});
	}

	public static int getNumCrawlingThreads() {
		return numCrawlingThreads;
	}

	public static void setNumCrawlingThreads(int numCrawlingThreads) {
		LazyProxyDirTreeDb.numCrawlingThreads = numCrawlingThreads;
	}

	public LazyProxyDirTreeDb (AbstractDirTreeDb parent) {
		super(parent);
	}

	@Override
	public void close() throws SQLException {
		Debug.writelog("Really closing DB");
		super.close();
		Debug.writelog("Closing lazyqueue_thread DB");
		lazyqueue_thread.close();
		Debug.writelog("Closing lazyqueue_insertable DB");
		lazyqueue_insertable.close();
		Debug.writelog("Closing lazyqueue_dontinsert DB");
		lazyqueue_dontinsert.close();
		Debug.writelog("LazyProxyDirTreeDb close finished");
	}

	@Override
	public void threadHook() throws InterruptedException {
		super.threadHook();
		((LazyAccessorThread.RunnerThread)Thread.currentThread()).threadHook();
	}

	@Override
	public int refreshIndirectUpperLower(Set<Long> dontListRootIds, IsEol isEol)
			throws SQLException, InterruptedException {
		Assertion.assertAssertionError(iAmLazyAccessorThread());
		Assertion.assertAssertionError(! isConsumeUpdateQueueMode());
		return super.refreshDirectUpperLower(dontListRootIds, isEol);
	}

	@Override
	public void insert(final DbPathEntry basedir, final PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		super.insert(basedir, newentry);
	}

	@Override
	public void delete(final DbPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		super.delete(entry);
	}

	@Deprecated @Override
	public void deleteChildren(final DbPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		super.deleteChildren(entry);
	}

	@Override
	public void orphanizeChildren(final DbPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		super.orphanizeChildren(entry);
	}

	@Override
	public void insertUpperLower(final long upper, final long lower, final int distance)
			throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		super.insertUpperLower(upper, lower, distance);
	}

	@Override
	public void deleteUpperLower(final long upper, final long lower) throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		super.deleteUpperLower(upper, lower);
	}

	@Override
	public void insertEquality(final long pathid1, final long pathid2, final long size, final int csum)
			throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		super.insertEquality(pathid1, pathid2, size, csum);
	}

	@Override
	public void consumeOneUpdateQueue() throws InterruptedException, SQLException {
		Assertion.assertAssertionError(iAmLazyAccessorThread());
		super.consumeOneUpdateQueue();
	}

	public void consumeSomeUpdateQueue() throws InterruptedException, SQLException {
		threadHook();
		while ((lazyqueue_insertable.size() > 0 || lazyqueue_dontinsert.size() > 0) && getUpdateQueueSize() > 0) {
			consumeOneUpdateQueue();
		}
	}

	public void consumeSomeUpdateQueueWithTimeLimit(long milliseconds) throws InterruptedException, SQLException {
		long t1 = new Date().getTime();
		threadHook();
		while ((lazyqueue_insertable.size() > 0 || lazyqueue_dontinsert.size() > 0) && getUpdateQueueSize() > 0) {
			consumeOneUpdateQueue();
			long t2 = new Date().getTime();
			if (t2 - t1 > milliseconds) {
				return;
			}
		}
	}

	private LazyQueue lazyqueue_insertable = new LazyQueue();
	private LazyQueue lazyqueue_dontinsert = new LazyQueue();
	private LazyQueueRunnerThreadPool lazyqueue_thread = new LazyQueueRunnerThreadPool();

	public int getInsertableQueueSize() {
		return lazyqueue_insertable.size();
	}

	public int getDontInsertQueueSize() {
		return lazyqueue_dontinsert.size();
	}

	public Set<Long> getInsertableRootIdSet() {
		return lazyqueue_insertable.getRootIdSet();
	}

	public Set<Long> getDontInsertRootIdSet() {
		return lazyqueue_dontinsert.getRootIdSet();
	}

	@Override
	public void discardAllQueueItems() {
		lazyqueue_thread.close();
		lazyqueue_insertable.discardAllItems();
		lazyqueue_dontinsert.discardAllItems();
		super.discardAllQueueItems();
	}

	@Deprecated
	private boolean iAmLazyAccessorThread() {
		try {
			return (LazyAccessorThread.RunnerThread)Thread.currentThread() != null;
		} catch (ClassCastException e) {
			return false;
		}
	}

	private class LazyQueueRunnerThreadPool extends ArrayList<LazyQueueRunnerThread> {

		public synchronized void wakeupThreadIfPossible() {
			// cleanup dead threads
			for (int i = size()-1; i >= 0; i--) {
				if (! get(i).isAlive()) {
					remove(i);
				}
			}

			if (size() < getNumCrawlingThreads()) {
				LazyQueueRunnerThread t = new LazyQueueRunnerThread();
				t.start();
				add(t);
			}
		}

		public synchronized void close() {
			for (int i = size()-1; i >= 0; i--) {
				if (get(i).isAlive()) {
					get(i).interrupt();
				}
			}
			for (int i = size()-1; i >= 0; i--) {
				if (get(i).isAlive()) {
					try {
						get(i).join();
					} catch (InterruptedException e) {}
				}
			}
		}
	}

	public abstract class LazyQueueableRunnable { // not really runnable, but used like that.
		abstract void run() throws SQLException, InterruptedException;
	}

	private class LazyQueueRunnerThread extends Thread {

		public void threadHook() throws InterruptedException {
			Thread.sleep(0); // throw InterruptedException if interrupted
		}

		public void run() {
			boolean isEol = false;
			while (! isEol) {
				isEol = true;
				for (LazyQueueElement target: lazyqueue_insertable.values()) {
					if (target.hasNext() && target.getThread() == null) {
						if (target.setThread((LazyQueueRunnerThread)Thread.currentThread())) {
							try {
								while (target.hasNext()) {
									Assertion.assertAssertionError(target.getThread() == Thread.currentThread());
									threadHook();
									target.next().run();
									isEol = false;
								}
							} catch (InterruptedException e) {
								return;
							} catch (SQLException e) {
								e.printStackTrace();
								System.exit(0); // this must not happen
							} finally {
								target.setThread(null);
							}
						}
					}
				}

				for (Entry<Long, LazyQueueElement> entry: lazyqueue_dontinsert.entrySet()) {
					long rootid = entry.getKey();
					LazyQueueElement target = entry.getValue();
					if (target.hasNext()
							&& target.getThread() == null
							&& lazyqueue_insertable.get(rootid).isEmpty()) {
						if (target.setThread((LazyQueueRunnerThread)Thread.currentThread())) {
							try {
								while (target.hasNext() && lazyqueue_insertable.get(rootid).isEmpty()) {
									Assertion.assertAssertionError(target.getThread() == Thread.currentThread());
									threadHook();
									target.next().run();
									isEol = false;
								}
							} catch (InterruptedException e) {
								return;
							} catch (SQLException e) {
								e.printStackTrace();
								System.exit(0); // this must not happen
							} finally {
								target.setThread(null);
							}
						}
					}
				}
			}
		}
	}

	private class LazyQueueElement extends AsynchronousProducerConsumerIterator<LazyQueueableRunnable> {
		LazyQueueRunnerThread thread = null;

		public synchronized boolean setThread(LazyQueueRunnerThread thread) {
			if (thread == null && this.thread == null) {
				return false;
			}
			if (thread != null && this.thread != null) {
				return false;
			}
			this.thread = thread;
			return true;
		}

		public synchronized LazyQueueRunnerThread getThread() {
			return thread;
		}

		public boolean isEmpty() {
			return size()==0 && getThread() == null;
		}

		public void enqueue(LazyQueueableRunnable newtodo) throws InterruptedException {
			add(newtodo);
			if (getThread() == null) {
				lazyqueue_thread.wakeupThreadIfPossible();
			}
		}
	}

	public class LazyQueue extends ConcurrentHashMap<Long, LazyQueueElement> {

		public LazyQueue() {
			super();
		}

		@Override
		public LazyQueueElement get(Object key) {
			LazyQueueElement result = super.get(key);
			if (result == null) {
				result = new LazyQueueElement();
				put((Long)key, result);
			}
			return result;
		}

		public boolean hasThread(Thread t) {
			for (LazyQueueElement elm: this.values()) {
				if (elm.getThread() == t) {
					return true;
				}
			}
			return false;
		}

		public void discardAllItems() {
			for (LazyQueueElement elm: this.values()) {
				LazyQueueRunnerThread t = elm.getThread();
				if (t != null) {
					t.interrupt();
				}
				while (elm.hasNext()) {
					elm.next();
				}
			}
		}

		public void enqueue(DbPathEntry entry, LazyQueueableRunnable newtodo) throws InterruptedException {
			long root;
			if (entry == null) {
				root = 0L;
			} else {
				root = entry.getRootId();
			}
			get(root).enqueue(newtodo);
		}

		public void noop() throws InterruptedException {
			for (LazyQueueElement element: values()) {
				element.enqueue(new LazyQueueableRunnable() {
					public void run() throws InterruptedException {
						LazyProxyDirTreeDb.this.noop();
					}
				});
			}
		}

		public void close() {
			for (LazyQueueElement element: values()) {
				element.close();
			}
		}

		@Override
		public int size() {
			int result = 0;
			for (LazyQueueElement element: values()) {
				result += element.size();
			}
			return result;
		}

		public int numElements() {
			return super.size();
		}

		public Set<Long> getRootIdSet() {
			Set<Long> result = new HashSet<Long>();
			for (java.util.Map.Entry<Long, LazyQueueElement> kv: entrySet()) {
				LazyQueueElement lqe = kv.getValue();
				if (!lqe.isEmpty()) {
					result.add(kv.getKey());
				}
			}
			return result;
		}

	}

	public Dispatcher getDispatcher() {
		return new Dispatcher();
	}

	public class Dispatcher extends ProxyDirTreeDb.Dispatcher {
		private boolean _noReturn = false;
		public void setNoReturn(boolean noReturn) { _noReturn = noReturn; }
		public boolean isNoReturn() { return _noReturn; }

		@Override
		public PathEntry dispatch(final DbPathEntry entry) throws IOException, InterruptedException, SQLException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			long t1 = (new Date()).getTime();
			try {
				if (entry == null) {
					return null;
				}
				if (isNoReturn()) {
					if (entry.isFolder()) {
						dispatchFolderNoReturn(entry);
					} else if (entry.isFile()) {
						dispatchFileNoReturn(entry);
					} else if (entry.isCompressedFile()) {
						dispatchCompressedFileNoReturn(entry);
					} else {
						// COMPRESSEDFOLDER - DON'T DO ANYTHING
					}
					return null;
				} else {
					if (entry.isFolder()) {
						return dispatchFolder(entry);
					} else if (entry.isFile()) {
						return dispatchFile(entry);
					} else if (entry.isCompressedFolder()) {
						return dispatchCompressedFolder(entry);
					} else if (entry.isCompressedFile()) {
						return dispatchCompressedFile(entry);
					} else {
						throw new AssertionError();
					}
				}
			} finally {
				long t2 = (new Date()).getTime();
				if (t2 - t1 > 30*1000) {
					long d = t2 - t1;
					Debug.writelog("dispatch time too long: " + d + " at " + entry.getPath());
				}
			}
		}

		private void dispatchFolderNoReturn(final DbPathEntry entry) throws InterruptedException, SQLException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			threadHook();

			final Map<String, DbPathEntry> oldfolder;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			LazyQueue lq = (oldfolder == null) ? lazyqueue_dontinsert : lazyqueue_insertable;
			lq.enqueue(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException {
					final PathEntry newentry;
					try {
						newentry = getNewPathEntry(entry);
					} catch (IOException e) {
						checkRootAndDisable(entry);
						return;
					}

					if (oldfolder == null) { // not isList()
						if (!entry.isDirty() && !PathEntry.dMatch(entry, newentry)) {
							updateStatus(entry, PathEntry.DIRTY);
						}
					} else { // isList()
						final File fileobj = getFileIfExists((PathEntry)entry);
						if (fileobj == null) {
							checkRootAndDisable(entry);
							return;
						}

						final DirLister newfolderIter;
						try {
							newfolderIter = new DirLister(entry, fileobj);
							dispatchFolderListCore(entry, fileobj, oldfolder, newentry, newfolderIter);
						} catch (IOException e) {
							checkRootAndDisable(entry);
							return;
						}
					}
				}
			});
		}

		private void dispatchFileNoReturn(final DbPathEntry entry) throws InterruptedException, SQLException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			threadHook();

			final Map<String, DbPathEntry> oldfolder;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			LazyQueue lq = (oldfolder == null) ? lazyqueue_dontinsert : lazyqueue_insertable;
			lq.enqueue(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException {
					final PathEntry newentry;
					try {
						newentry = getNewPathEntry(entry);
					} catch (IOException e) {
						checkRootAndDisable(entry);
						return;
					}

					try {
						if (!PathEntry.dscMatch(entry, newentry)) {
							unsetClean(entry.getParentId());
						}

						if (oldfolder == null) { // not isList()
							if ((!entry.isDirty() && !PathEntry.dscMatch(entry, newentry)) || entry.isNoAccess()) {
								newentry.setStatus(PathEntry.DIRTY);
							}
						} else { // isList()
							final PathEntryLister newfolderIter;
							newfolderIter = PathEntryListerFactory.getInstance(entry);
							newfolderIter.setCsumRequested(PathEntryListerFactory.isCsumRecommended(entry));
							dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
						}
						if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !PathEntry.dscMatch(entry, newentry)))) {
							newentry.setCsumAndClose(newentry.getInputStream());
							if (newentry.isNoAccess()) {
								newentry.setStatus(PathEntry.DIRTY);
							}
						}
					} catch (IOException e) {
						checkRootAndDisable(entry);
						return;
					}
					update(entry, newentry);
				}
			});
		}

		private void dispatchCompressedFileNoReturn(final DbPathEntry entry) throws SQLException, InterruptedException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			threadHook();

			final Map<String, DbPathEntry> oldfolder;
			LazyQueue lq = isList() ? lazyqueue_insertable : lazyqueue_dontinsert;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			final List<DbPathEntry> stack = getCompressionStack(entry);
			if (stack == null) { return; /* orphan */ }

			lq.enqueue(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException
				{
					PathEntry newentry = new PathEntry(entry);
					try {
						PathEntryLister newfolderIter;
						InputStream inf;

						if (oldfolder != null) {
							newfolderIter = PathEntryListerFactory.getInstance(entry, getInputStream(stack));
							newfolderIter.setCsumRequested(PathEntryListerFactory.isCsumRecommended(entry));
							dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
						} else {
							newfolderIter = null;
						}
						if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !PathEntry.dscMatch(entry, newentry)))) {
							assert(stack != null);
							inf = getInputStream(stack);
							if (newentry.isNoAccess()) {
								newentry.setStatus(PathEntry.DIRTY);
							}
						} else {
							inf = null;
						}

						if (inf != null) {
							newentry.setCsumAndClose(inf);
						}
					} catch (IOException e) {
						checkRootAndDisable(entry);
						return;
					}
					update(entry, newentry);
				}
			});
		}

		protected PathEntry dispatchFolder(final DbPathEntry entry) throws InterruptedException, SQLException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			threadHook();

			final File fileobj = getFileIfExists(entry);
			if (fileobj == null) {
				checkRootAndDisable(entry);
				return null;
			}

			final PathEntry newentry;
			try {
				newentry = getNewPathEntry(entry, fileobj);
			} catch (IOException e) {
				checkRootAndDisable(entry);
				return null;
			}

			newentry.setSize(entry.getSize());
			newentry.setCompressedSize(entry.getCompressedSize());

			if (entry.isClean() && PathEntry.dMatch(entry, newentry)) {
				return newentry; // no change
			}

			final Map<String, DbPathEntry> oldfolder;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			LazyQueue lq = (oldfolder == null) ? lazyqueue_dontinsert : lazyqueue_insertable;
			lq.enqueue(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException
				{
					if (!isList()) {
						if (!entry.isDirty() && !PathEntry.dMatch(entry, newentry)) {
							updateStatus(entry, PathEntry.DIRTY);
						}
					} else if (isList() && oldfolder != null) {
						final DirLister newfolderIter;
						try {
							newfolderIter = new DirLister(entry, fileobj);
							dispatchFolderListCore(entry, fileobj, oldfolder, newentry, newfolderIter);
						} catch (IOException e) {
							checkRootAndDisable(entry);
							return;
						}
					}
				}
			});
			return newentry;
		}

		protected PathEntry dispatchFile(final DbPathEntry entry) throws InterruptedException, SQLException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			threadHook();

			final PathEntry newentry;
			try {
				newentry = getNewPathEntry(entry);
			} catch (IOException e) {
				checkRootAndDisable(entry);
				return null;
			}

			final Map<String, DbPathEntry> oldfolder;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			LazyQueue lq = (oldfolder == null) ? lazyqueue_dontinsert : lazyqueue_insertable;
			lq.enqueue(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException
				{
					try {
						if (!PathEntry.dscMatch(entry, newentry)) {
							unsetClean(entry.getParentId());
						}

						if (oldfolder == null) {
							if (!entry.isDirty() && !PathEntry.dscMatch(entry, newentry)) {
								newentry.setStatus(PathEntry.DIRTY);
							}
						} else {
							final PathEntryLister newfolderIter;
							newfolderIter = PathEntryListerFactory.getInstance(entry);
							newfolderIter.setCsumRequested(PathEntryListerFactory.isCsumRecommended(entry));
							dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
						}
						if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !PathEntry.dscMatch(entry, newentry)))) {
							newentry.setCsumAndClose(newentry.getInputStream());
							if (newentry.isNoAccess()) {
								newentry.setStatus(PathEntry.DIRTY);
							}
						}
					} catch (IOException e) {
						checkRootAndDisable(entry);
						return;
					}
					update(entry, newentry);
				}
			});
			return newentry;
		}

		protected PathEntry dispatchCompressedFolder(final DbPathEntry entry) throws SQLException, InterruptedException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			threadHook();

			final List<DbPathEntry> stack = getCompressionStack(entry);
			if (stack == null) { return null; /* orphan */ }
			try {
				DbPathEntry p1 = stack.get(stack.size()-1);
				PathEntry p2 = getNewPathEntry(p1);
				if (!PathEntry.dscMatch(p1, p2)) {
					updateStatus(p1, PathEntry.DIRTY);
				}
			} catch (IOException e) {
				checkRootAndDisable(entry);
				return null;
			}

			return entry;
		}

		protected PathEntry dispatchCompressedFile(final DbPathEntry entry) throws SQLException, InterruptedException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			threadHook();

			final List<DbPathEntry> stack = getCompressionStack(entry);
			if (stack == null) { return null; /* orphan */ }
			try {
				DbPathEntry p1 = stack.get(stack.size()-1);
				PathEntry p2 = getNewPathEntry(p1);
				if (!PathEntry.dscMatch(p1, p2)) {
					updateStatus(p1, PathEntry.DIRTY);
				}
			} catch (IOException e) {
				checkRootAndDisable(entry);
				return null;
			}

			final Map<String, DbPathEntry> oldfolder;
			LazyQueue lq = isList() ? lazyqueue_insertable : lazyqueue_dontinsert;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			lq.enqueue(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException
				{
					PathEntry newentry = new PathEntry(entry);
					try {
						PathEntryLister newfolderIter;
						InputStream inf;

						if (oldfolder != null) {
							newfolderIter = PathEntryListerFactory.getInstance(entry, getInputStream(stack));
							newfolderIter.setCsumRequested(PathEntryListerFactory.isCsumRecommended(entry));
							dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
						} else {
							newfolderIter = null;
						}
						if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !PathEntry.dscMatch(entry, newentry)))) {
							assert(stack != null);
							inf = getInputStream(stack);
							if (newentry.isNoAccess()) {
								newentry.setStatus(PathEntry.DIRTY);
							}
						} else {
							inf = null;
						}

						if (inf != null) {
							newentry.setCsumAndClose(inf);
						}
					} catch (IOException e) {
						checkRootAndDisable(entry);
						return;
					}
					update(entry, newentry);
				}
			});
			return entry;
		}

		@Override
		public boolean checkEquality(final DbPathEntry entry1, final DbPathEntry entry2, final int dbAccessMode)
				throws SQLException, InterruptedException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			if (isNoReturn()) {
				checkEqualityNoReturn(entry1, entry2, dbAccessMode);
				return true;
			} else {
				return super.checkEquality(entry1, entry2, dbAccessMode);
			}
		}

		protected void checkEqualityNoReturn(final DbPathEntry entry1, final DbPathEntry entry2, final int dbAccessMode)
				throws SQLException, InterruptedException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			if (entry1 == null || ! isReachableRoot(entry1.getRootId())) { return; }
			if (entry2 == null || ! isReachableRoot(entry2.getRootId())) { return; }

			final List<DbPathEntry> stack1 = getCompressionStack(entry1);
			if (stack1 == null) { // orphan
				if (dbAccessMode == CHECKEQUALITY_UPDATE || dbAccessMode == CHECKEQUALITY_AUTOSELECT) {
					deleteEquality(entry1.getPathId(), entry2.getPathId());
				}
				return;
			}
			final List<DbPathEntry> stack2 = getCompressionStack(entry2);
			if (stack2 == null) { // orphan
				if (dbAccessMode == CHECKEQUALITY_UPDATE || dbAccessMode == CHECKEQUALITY_AUTOSELECT) {
					deleteEquality(entry1.getPathId(), entry2.getPathId());
				}
				return;
			}
			checkEqualityNoReturn(stack1, stack2, dbAccessMode);
		}

		@Override
		public boolean checkEquality(
				final List<DbPathEntry> stack1,
				final List<DbPathEntry> stack2,
				final int dbAccessMode
				) throws SQLException, InterruptedException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			if (isNoReturn()) {
				checkEqualityNoReturn(stack1, stack2, dbAccessMode);
				return true;
			} else {
				return super.checkEquality(stack1, stack2, dbAccessMode);
			}
		}

		protected void checkEqualityNoReturn(
				final List<DbPathEntry> stack1,
				final List<DbPathEntry> stack2,
				final int dbAccessMode
				) throws SQLException, InterruptedException {
			Assertion.assertAssertionError(! lazyqueue_insertable.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			if (dbAccessMode == CHECKEQUALITY_NONE) { return; }
			if (stack1 == null || stack2 == null) { return; /* orphan */ }
			final DbPathEntry entry1 = stack1.get(0);
			final DbPathEntry entry2 = stack2.get(0);

			Assertion.assertAssertionError(entry1.isFile() || entry1.isCompressedFile(),
					"wrong type " + entry1.getType() + " for checkEquality: path=" + entry1.getPath());
			Assertion.assertAssertionError(entry2.isFile() || entry2.isCompressedFile(),
					"wrong type " + entry2.getType() + " for checkEquality: path=" + entry2.getPath());
			Assertion.assertAssertionError(entry1.getSize() == entry2.getSize());
			Assertion.assertAssertionError(!entry1.isCsumNull());
			Assertion.assertAssertionError(!entry2.isCsumNull());
			Assertion.assertAssertionError(entry1.getCsum() == entry2.getCsum());

			DbPathEntry p = (entry1.getRootId() == entry2.getRootId()) ? entry1 : null;
			LazyQueue lq = dbAccessMode == CHECKEQUALITY_INSERT ? lazyqueue_insertable : lazyqueue_dontinsert;
			lq.enqueue(p, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException {
					Dispatcher.super.checkEquality(stack1, stack2, dbAccessMode);
				}
			});
		}
	}

	public void writelog2(final String message) {
		ArrayList<String> is = new ArrayList<String>();
		for (java.util.Map.Entry<Long, LazyQueueElement> kv: lazyqueue_insertable.entrySet()) {
			if (kv.getValue().size()>0) {
				is.add(kv.getKey()+"="+kv.getValue().size());
			}
		}
		String iss = is.size()==0 ? "" : "("+String.join(", ", is)+")";
		ArrayList<String> ds = new ArrayList<String>();
		for (java.util.Map.Entry<Long, LazyQueueElement> kv: lazyqueue_dontinsert.entrySet()) {
			if (kv.getValue().size()>0) {
				ds.add(kv.getKey()+"="+kv.getValue().size());
			}
		}
		String dss = ds.size()==0 ? "" : "("+String.join(", ", ds)+")";

		System.out.println(String.format("%s qL=%d %s qC=%d %s qS=%d+%d qT=%d %s",
				new Date().toString(),
				lazyqueue_insertable.size(),
				iss,
				lazyqueue_dontinsert.size(),
				dss,
				getUpdateQueueSize(0),
				getUpdateQueueSize(1),
				lazyqueue_thread.size(),
				message));
	}

}
