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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LazyProxyDirTreeDb extends ProxyDirTreeDb {

	private static int numCrawlingThreads = 1;

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
		lazyqueue_thread.close();
		lazyqueue_insertable.close();
		lazyqueue_dontinsert.close();
		super.close();
	}

	@Override
	public void threadHook() throws InterruptedException {
		super.threadHook();

		LazyAccessorThread.RunnerThread t = (LazyAccessorThread.RunnerThread)Thread.currentThread();
		t.threadHook();
	}

	@Override
	public void refreshDirectUpperLower() throws SQLException, InterruptedException {
		refreshDirectUpperLower(getInsertableRootIdList());
	}

	@Override
	public void refreshIndirectUpperLower() throws SQLException, InterruptedException {
		refreshIndirectUpperLower(getInsertableRootIdList());
	}

	@Override
	public void insert(final DbPathEntry basedir, final PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(newentry != null);
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.insert(basedir, newentry);
			}
		});
	}

	@Override
	public void update(final DbPathEntry oldentry, final PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(oldentry != null);
		Assertion.assertNullPointerException(newentry != null);
		Assertion.assertAssertionError(oldentry.getPath().equals(newentry.getPath()),
				"!! old and new entry paths do not match:\nold=" + oldentry.getPath() + "\nnew=" + newentry.getPath());
		Assertion.assertAssertionError(oldentry.getType() == newentry.getType());

		if (! dscMatch(oldentry, newentry)
				|| oldentry.isCsumNull() != newentry.isCsumNull()
				|| (!oldentry.isCsumNull() && !newentry.isCsumNull() && oldentry.getCsum() != newentry.getCsum())
				|| oldentry.getStatus() != newentry.getStatus()
				) {
			updatequeue.execute(new UpdateQueueableRunnable () {
				public void run() throws SQLException, InterruptedException {
					LazyProxyDirTreeDb.super.update(oldentry, newentry);
				}
			});
		}
	}

	@Override
	public void updateStatus(final DbPathEntry entry, final int newstatus)
			throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.updateStatus(entry, newstatus);
			}
		});
	}

	@Override
	public void delete(final DbPathEntry entry) throws SQLException,
			InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.delete(entry);
			}
		});
	}

	@Override
	protected void delete_LowPriority(final DbPathEntry entry) throws SQLException,
	InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.delete(entry);
			}
		}, 1);
	}

	@Override
	public void deleteChildren(final DbPathEntry entry)
			throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.deleteChildren(entry);
			}
		});
	}

	@Override
	public void disable(final DbPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.disable(entry);
			}
		});
	}

	@Override
	public void disable(final DbPathEntry entry, final PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertNullPointerException(newentry != null);
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.disable(entry, newentry);
			}
		});
	}

	public void noop() throws InterruptedException {
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				// noop
			}
		});
	}

	@Override
	public void insertUpperLower(final long upper, final long lower, final int distance)
			throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.insertUpperLower(upper, lower, distance);
			}
		});
	}

	@Override
	public void deleteUpperLower(final long upper, final long lower) throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.deleteUpperLower(upper, lower);
			}
		});
	}

	@Override
	public void deleteUpperLower(final long pathid) throws SQLException, InterruptedException {
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.deleteUpperLower(pathid);
			}
		});
	}

	@Override
	public void insertEquality(final long pathid1, final long pathid2, final long size, final int csum)
			throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! lazyqueue_dontinsert.hasThread(Thread.currentThread()));
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.insertEquality(pathid1, pathid2, size, csum);
			}
		});
	}

	@Override
	public void deleteEquality(final long pathid1, final long pathid2) throws InterruptedException, SQLException {
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.deleteEquality(pathid1, pathid2);
			}
		});
	}

	@Override
	public void updateEquality(final long pathid1, final long pathid2) throws InterruptedException, SQLException {
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.updateEquality(pathid1, pathid2);
			}
		});
	}

	@Override
	public void updateDuplicateFields(final long pathid, final long duplicate, final long dedupablesize)
			throws InterruptedException, SQLException {
		updatequeue.execute(new UpdateQueueableRunnable () {
			public void run() throws SQLException, InterruptedException {
				LazyProxyDirTreeDb.super.updateDuplicateFields(pathid, duplicate, dedupablesize);
			}
		});
	}

	public abstract class UpdateQueueableRunnable { // not really runnable, but used like that.
		abstract void run() throws SQLException, InterruptedException;
	}

	public class UpdateQueue extends AsynchronousProducerConsumerIteratorWithPriority<UpdateQueueableRunnable> {
		public UpdateQueue() {
			super(2);
		}

		public void execute(UpdateQueueableRunnable newtodo) throws InterruptedException {
			add(newtodo);
		}

		public void execute(UpdateQueueableRunnable newtodo, int priority) throws InterruptedException {
			add(newtodo, priority);
		}
	}

	private LazyQueue lazyqueue_insertable = new LazyQueue();
	private LazyQueue lazyqueue_dontinsert = new LazyQueue();
	private LazyQueueRunnerThreadPool lazyqueue_thread = new LazyQueueRunnerThreadPool();
	private UpdateQueue updatequeue = new UpdateQueue();
	
	public int getInsertableQueueSize() {
		return lazyqueue_insertable.size();
	}

	public int getDontInsertQueueSize() {
		return lazyqueue_dontinsert.size();
	}

	public int getUpdateQueueSize() {
		return updatequeue.size();
	}

	public List<Long> getInsertableRootIdList() {
		return lazyqueue_insertable.getRootIdList();
	}

	public List<Long> getDontInsertRootIdList() {
		return lazyqueue_dontinsert.getRootIdList();
	}

	public void discardAllQueueItems() {
		lazyqueue_thread.close();
		lazyqueue_insertable.discardAllItems();
		lazyqueue_dontinsert.discardAllItems();
		while (updatequeue.size() > 0) {
			updatequeue.next();
		}
	}

	private boolean iAmLazyAccessorThread() {
		try {
			return (LazyAccessorThread.RunnerThread)Thread.currentThread() != null;
		} catch (ClassCastException e) {
			return false;
		}
	}

	public void consumeOneUpdateQueue() throws InterruptedException, SQLException {
		Assertion.assertAssertionError(iAmLazyAccessorThread());
		threadHook();
		if (updatequeue.hasNext()) {
			UpdateQueueableRunnable o = updatequeue.previewNext();
			try {
				o.run();
			} finally {
				updatequeue.next();
			}
		}
	}

	public void consumeUpdateQueue() throws InterruptedException, SQLException {
		threadHook();
		noop();
		while (updatequeue.size() > 0) {
			consumeOneUpdateQueue();
		}
	}

	public void consumeSomeUpdateQueue() throws InterruptedException, SQLException {
		threadHook();
		while ((lazyqueue_insertable.size() > 0 || lazyqueue_dontinsert.size() > 0) && updatequeue.size() > 0) {
			consumeOneUpdateQueue();
		}
	}

	public void consumeSomeUpdateQueueWithTimeLimit(long milliseconds) throws InterruptedException, SQLException {
		long t1 = new Date().getTime();
		threadHook();
		while ((lazyqueue_insertable.size() > 0 || lazyqueue_dontinsert.size() > 0) && updatequeue.size() > 0) {
			consumeOneUpdateQueue();
			long t2 = new Date().getTime();
			if (t2 - t1 > milliseconds) {
				return;
			}
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

		boolean isInterrupted = false;

		public void run() {
			while (true) {
				LazyQueueElement target = null;
				LazyQueue[] lql = { lazyqueue_insertable, lazyqueue_dontinsert };
				LOOP1: for (LazyQueue lq: lql) {
					for (LazyQueueElement lqe: lq.values()) {
						if (lqe.size() > 0 && lqe.getThread() == null) {
							target = lqe;
							target.setThread((LazyQueueRunnerThread)Thread.currentThread());
							break LOOP1;
						}
					}
				}
				if (target == null) {
					return;
				}
				try {
					while (target.size() > 0) {
						if (isInterrupted) {
							throw new InterruptedException("!! DirTreeDBLazyQueueElement thread interrupted");
						}
						LazyQueueableRunnable o = target.previewNext();
						try {
							o.run();
						} finally {
							target.next();
						}
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

		public void interrupt() {
			isInterrupted = true;
		}
	}

	private class LazyQueueElement extends AsynchronousProducerConsumerIterator<LazyQueueableRunnable> {
		LazyQueueRunnerThread thread = null;

		public synchronized void setThread(LazyQueueRunnerThread thread) {
			this.thread = thread;
		}

		public LazyQueueRunnerThread getThread() {
			return thread;
		}

		public void execute(LazyQueueableRunnable newtodo) throws InterruptedException {
			add(newtodo);
			if (getThread() == null) {
				lazyqueue_thread.wakeupThreadIfPossible();
			}
		}

	}

	public class LazyQueue extends ConcurrentHashMap<Long, LazyQueueElement> {

		public LazyQueue() {
			super();
			put(0L, new LazyQueueElement());
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
				while (elm.size() > 0) {
					elm.next();
				}
			}
		}

		public void execute(DbPathEntry entry, LazyQueueableRunnable newtodo) throws InterruptedException {
			long root;
			if (entry == null) {
				root = 0L;
			} else {
				root = entry.getRootId();
				if (! containsKey(root)) {
					put(root, new LazyQueueElement());
				}
			}
			get(root).execute(newtodo);
		}

		public void noop() throws InterruptedException {
			for (LazyQueueElement element: values()) {
				element.execute(new LazyQueueableRunnable() {
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

		public ArrayList<Long> getRootIdList() {
			ArrayList<Long> result = new ArrayList<Long>();
			for (java.util.Map.Entry<Long, LazyQueueElement> kv: entrySet()) {
				if (kv.getValue().size()>0) {
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
		protected boolean _noReturn = false;
		public void setNoReturn(boolean noReturn) { _noReturn = noReturn; }
		public boolean isNoReturn() { return _noReturn; }

		@Override
		public PathEntry dispatch(final DbPathEntry entry) throws IOException, InterruptedException, SQLException {
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
					writelog("dispatch time too long: " + d + " at " + entry.getPath());
				}
			}
		}

		private void dispatchFolderNoReturn(final DbPathEntry entry) throws InterruptedException, SQLException {
			threadHook();

			final HashMap<String, DbPathEntry> oldfolder;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			LazyQueue lq = (oldfolder == null) ? lazyqueue_dontinsert : lazyqueue_insertable;
			lq.execute(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException {
					final PathEntry newentry;
					try {
						newentry = getNewPathEntry(entry);
					} catch (IOException e) {
						disable(entry);
						return;
					}

					if (oldfolder == null) { // not isList()
						if (!entry.isDirty() && !dMatch(entry, newentry)) {
							updateStatus(entry, PathEntry.DIRTY);
						}
					} else { // not isList()
						final File fileobj = getFileIfExists((PathEntry)entry);
						if (fileobj == null) {
							disable(entry, newentry);
							return;
						}

						final DirLister newfolderIter;
						try {
							newfolderIter = new DirLister(entry, fileobj);
							dispatchFolderListCore(entry, fileobj, oldfolder, newentry, newfolderIter);
						} catch (IOException e) {
							disable(entry, newentry);
							return;
						}
					}
				}
			});
		}

		private void dispatchFileNoReturn(final DbPathEntry entry) throws InterruptedException, SQLException {
			threadHook();

			final HashMap<String, DbPathEntry> oldfolder;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			LazyQueue lq = (oldfolder == null) ? lazyqueue_dontinsert : lazyqueue_insertable;
			lq.execute(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException {
					final PathEntry newentry;
					try {
						newentry = getNewPathEntry(entry);
					} catch (IOException e) {
						disable(entry);
						return;
					}

					try {
						if (oldfolder == null) { // not isList()
							if ((!entry.isDirty() && !dscMatch(entry, newentry)) || entry.isNoAccess()) {
								newentry.setStatus(PathEntry.DIRTY);
							}
						} else { // isList()
							final IArchiveLister newfolderIter;
							newfolderIter = ArchiveListerFactory.getArchiveLister(entry, newentry.getInputStream());
							dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
						}
						if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !dscMatch(entry, newentry)))) {
							newentry.setCsumAndClose(newentry.getInputStream());
							if (newentry.isNoAccess()) {
								newentry.setStatus(PathEntry.DIRTY);
							}
						}
					} catch (IOException e) {
						disable(entry, newentry);
						return;
					} catch (OutOfMemoryError e) {
						writelog("!! WARNING !! Caught OutOfMemoryError at: " + entry.getPath());
						e.printStackTrace();
						disable(entry, newentry);
						return;
					}
					update(entry, newentry);
				}
			});
		}

		private void dispatchCompressedFileNoReturn(final DbPathEntry entry) throws SQLException, InterruptedException {
			threadHook();

			final HashMap<String, DbPathEntry> oldfolder;
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

			lq.execute(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException
				{
					PathEntry newentry = new PathEntry(entry);
					try {
						IArchiveLister newfolderIter;
						InputStream inf;

						if (oldfolder != null) {
							newfolderIter = ArchiveListerFactory.getArchiveLister(entry, getInputStream(stack));
							dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
						} else {
							newfolderIter = null;
						}
						if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !dscMatch(entry, newentry)))) {
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
						disable(entry);
						return;
					} catch (OutOfMemoryError e) {
						writelog("!! WARNING !! Caught OutOfMemoryError at: " + entry.getPath());
						e.printStackTrace();
						disable(entry, newentry);
						return;
					}
					update(entry, newentry);
				}
			});
		}

		protected PathEntry dispatchFolder(final DbPathEntry entry) throws InterruptedException, SQLException {
			threadHook();

			final File fileobj = getFileIfExists(entry);
			if (fileobj == null) {
				disable(entry);
				return null;
			}

			final PathEntry newentry;
			try {
				newentry = new PathEntry(fileobj);
			} catch (IOException e) {
				disable(entry);
				return null;
			}

			newentry.setSize(entry.getSize());
			newentry.setCompressedSize(entry.getCompressedSize());

			if (entry.isClean() && dMatch(entry, newentry)) {
				return newentry; // no change
			}

			final HashMap<String, DbPathEntry> oldfolder;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			LazyQueue lq = (oldfolder == null) ? lazyqueue_dontinsert : lazyqueue_insertable;
			lq.execute(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException
				{
					if (!isList()) {
						if (!entry.isDirty() && !dMatch(entry, newentry)) {
							updateStatus(entry, PathEntry.DIRTY);
						}
					} else if (isList() && oldfolder != null) {
						final DirLister newfolderIter;
						try {
							newfolderIter = new DirLister(entry, fileobj);
							dispatchFolderListCore(entry, fileobj, oldfolder, newentry, newfolderIter);
						} catch (IOException e) {
							disable(entry);
							return;
						}
					}
				}
			});
			return newentry;
		}

		protected PathEntry dispatchFile(final DbPathEntry entry) throws InterruptedException, SQLException {
			threadHook();

			final PathEntry newentry;
			try {
				newentry = getNewPathEntry(entry);
			} catch (IOException e) {
				disable(entry);
				return null;
			}

			final HashMap<String, DbPathEntry> oldfolder;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			LazyQueue lq = (oldfolder == null) ? lazyqueue_dontinsert : lazyqueue_insertable;
			lq.execute(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException
				{
					try {
						if (oldfolder == null) {
							if (!entry.isDirty() && !dscMatch(entry, newentry)) {
								newentry.setStatus(PathEntry.DIRTY);
							}
						} else {
							final IArchiveLister newfolderIter;
							newfolderIter = ArchiveListerFactory.getArchiveLister(entry, newentry.getInputStream());
							dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
						}
						if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !dscMatch(entry, newentry)))) {
							newentry.setCsumAndClose(newentry.getInputStream());
							if (newentry.isNoAccess()) {
								newentry.setStatus(PathEntry.DIRTY);
							}
						}
					} catch (IOException e) {
						disable(entry, newentry);
						return;
					} catch (OutOfMemoryError e) {
						writelog("!! WARNING !! Caught OutOfMemoryError at: " + entry.getPath());
						e.printStackTrace();
						disable(entry, newentry);
						return;
					}
					update(entry, newentry);
				}
			});
			return newentry;
		}

		protected PathEntry dispatchCompressedFolder(final DbPathEntry entry) throws SQLException, InterruptedException {
			threadHook();

			final List<DbPathEntry> stack = getCompressionStack(entry);
			if (stack == null) { return null; /* orphan */ }
			try {
				DbPathEntry p1 = stack.get(stack.size()-1);
				PathEntry p2 = getNewPathEntry(p1);
				if (!dscMatch(p1, p2)) {
					updateStatus(p1, PathEntry.DIRTY);
				}
			} catch (IOException e) {
				disable(entry);
				return null;
			}

			return entry;
		}

		protected PathEntry dispatchCompressedFile(final DbPathEntry entry) throws SQLException, InterruptedException {
			threadHook();

			final List<DbPathEntry> stack = getCompressionStack(entry);
			if (stack == null) { return null; /* orphan */ }
			try {
				DbPathEntry p1 = stack.get(stack.size()-1);
				PathEntry p2 = getNewPathEntry(p1);
				if (!dscMatch(p1, p2)) {
					updateStatus(p1, PathEntry.DIRTY);
				}
			} catch (IOException e) {
				disable(entry);
				return null;
			}

			final HashMap<String, DbPathEntry> oldfolder;
			LazyQueue lq = isList() ? lazyqueue_insertable : lazyqueue_dontinsert;
			if (!isList()) {
				oldfolder = null;
			} else if (isNoChildInDb()) {
				oldfolder = new HashMap<String, DbPathEntry>();
			} else {
				oldfolder = childrenList(entry);
			}

			lq.execute(entry, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException
				{
					PathEntry newentry = new PathEntry(entry);
					try {
						IArchiveLister newfolderIter;
						InputStream inf;

						if (oldfolder != null) {
							newfolderIter = ArchiveListerFactory.getArchiveLister(entry, getInputStream(stack));
							dispatchFileListCore(entry, oldfolder, newentry, newfolderIter);
						} else {
							newfolderIter = null;
						}
						if (isCsumForce() || (isCsum() && (entry.isCsumNull() || !dscMatch(entry, newentry)))) {
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
						disable(entry);
						return;
					} catch (OutOfMemoryError e) {
						writelog("!! WARNING !! Caught OutOfMemoryError at: " + entry.getPath());
						e.printStackTrace();
						disable(entry, newentry);
						return;
					}
					update(entry, newentry);
				}
			});
			return entry;
		}

		@Override
		public boolean checkEquality(final DbPathEntry entry1, final DbPathEntry entry2, final boolean inserting)
				throws SQLException, InterruptedException {
			if (isNoReturn()) {
				checkEqualityNoReturn(entry1, entry2, inserting);
				return true;
			} else {
				return super.checkEquality(entry1, entry2, inserting);
			}
		}

		protected void checkEqualityNoReturn(final DbPathEntry entry1, final DbPathEntry entry2, final boolean inserting)
				throws SQLException, InterruptedException {
			Assertion.assertAssertionError(iAmLazyAccessorThread());

			if (entry1 == null || entry2 == null) { return; }

			final List<DbPathEntry> stack1 = getCompressionStack(entry1);
			if (stack1 == null) { return; /* orphan */ }
			final List<DbPathEntry> stack2 = getCompressionStack(entry2);
			if (stack2 == null) { return; /* orphan */ }

			checkEqualityNoReturn(stack1, stack2, inserting);
		}

		@Override
		public boolean checkEquality(
				final List<DbPathEntry> stack1,
				final List<DbPathEntry> stack2,
				final boolean inserting
				) throws SQLException, InterruptedException {
			if (isNoReturn()) {
				checkEqualityNoReturn(stack1, stack2, inserting);
				return true;
			} else {
				return super.checkEquality(stack1, stack2, inserting);
			}
		}

		protected void checkEqualityNoReturn(
				final List<DbPathEntry> stack1,
				final List<DbPathEntry> stack2,
				final boolean inserting
				) throws SQLException, InterruptedException {
			if (stack1 == null || stack2 == null) { return; /* orphan */ }
			DbPathEntry entry1 = stack1.get(0);
			DbPathEntry entry2 = stack2.get(0);

			Assertion.assertAssertionError(entry1.isFile() || entry1.isCompressedFile(),
					"wrong type " + entry1.getType() + " for checkEquality: path=" + entry1.getPath());
			Assertion.assertAssertionError(entry2.isFile() || entry2.isCompressedFile(),
					"wrong type " + entry2.getType() + " for checkEquality: path=" + entry2.getPath());
			Assertion.assertAssertionError(entry1.getSize() == entry2.getSize());
			Assertion.assertAssertionError(!entry1.isCsumNull());
			Assertion.assertAssertionError(!entry2.isCsumNull());
			Assertion.assertAssertionError(entry1.getCsum() == entry2.getCsum());

			DbPathEntry p = (entry1.getRootId() == entry2.getRootId()) ? entry1 : null;
			LazyQueue lq = inserting ? lazyqueue_insertable : lazyqueue_dontinsert;
			lq.execute(p, new LazyQueueableRunnable() {
				public void run() throws SQLException, InterruptedException {
					Dispatcher.super.checkEquality(stack1, stack2, inserting);
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
				updatequeue.size(0),
				updatequeue.size(1),
				lazyqueue_thread.size(),
				message));
	}

}
