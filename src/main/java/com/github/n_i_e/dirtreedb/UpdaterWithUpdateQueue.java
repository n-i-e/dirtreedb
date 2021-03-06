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

import java.sql.SQLException;
import java.util.Set;

public class UpdaterWithUpdateQueue extends Updater {

	public UpdaterWithUpdateQueue(IDirTreeDB parent) {
		super(parent);
	}

	@Override
	public void close() throws SQLException {
		updatequeue.close();
		super.close();
	}

	public boolean isOpen() {
		return updatequeue.isOpen();
	}

	/*
	 * UpdateQueue entity and related methods
	 */

	public class UpdateQueue extends IterableQueueWithPriority<RunnableWithException2<SQLException, InterruptedException>> {
		public UpdateQueue() {
			super(2);
		}

		public void enqueue(RunnableWithException2<SQLException, InterruptedException> newtodo) throws InterruptedException {
			add(newtodo);
		}

		public void enqueue(RunnableWithException2<SQLException, InterruptedException> newtodo, int priority) throws InterruptedException {
			add(newtodo, priority);
		}
	}

	private UpdateQueue updatequeue = new UpdateQueue();

	public void enqueueUpdate(RunnableWithException2<SQLException, InterruptedException> newtodo)
			throws InterruptedException {
		updatequeue.enqueue(newtodo);
	}

	public void enqueueUpdate(RunnableWithException2<SQLException, InterruptedException> newtodo, int priority)
			throws InterruptedException {
		updatequeue.enqueue(newtodo, priority);
	}

	public int getUpdateQueueSize() {
		return updatequeue.size();
	}

	public int getUpdateQueueSize(int priority) {
		return updatequeue.size(priority);
	}

	/*
	 * consumeOneUpdateQueue() and related
	 */

	/**
	 * consumeUpdateQueueMode is enabled when you start consumeOneUpdateQueue(), and disabled when you finish it.
	 */
	private Thread consumeUpdateQueueModeThread = null;

	public boolean isConsumeUpdateQueueMode() {
		if (consumeUpdateQueueModeThread == null) {
			return false;
		}
		if (consumeUpdateQueueModeThread == Thread.currentThread()) {
			return true;
		} else {
			return false;
		}
	}

	private void beginConsumeUpdateQueueMode() {
		if (consumeUpdateQueueModeThread != null) {
			throw new ConsumeUpdateQueueModeError();
		}
		consumeUpdateQueueModeThread = Thread.currentThread();
	}

	private void endConsumeUpdateQueueMode() {
		consumeUpdateQueueModeThread = null;
	}

	public synchronized void consumeOneUpdateQueue() throws InterruptedException, SQLException {
		Assertion.assertNullPointerException(isOpen(), "!! Update Queue Already Closed");
		if (updatequeue.hasNext()) {
			try {
				beginConsumeUpdateQueueMode();
				updatequeue.next().run();
			} finally {
				endConsumeUpdateQueueMode();
			}
		}
	}

	public void consumeUpdateQueue() throws InterruptedException, SQLException {
		while (updatequeue.size() > 0) {
			consumeOneUpdateQueue();
		}
	}

	public void consumeUpdateQueue(int priority) throws InterruptedException, SQLException {
		while (updatequeue.size(priority) > 0) {
			consumeOneUpdateQueue();
		}
	}

	private static class ConsumeUpdateQueueModeError extends Error {}

	/*
	 * overriding metohds to write DirTreeDB; they are not directly written but enqueued.
	 */

	@Override
	public void insert(final DBPathEntry basedir, final PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(newentry != null);
		if (isConsumeUpdateQueueMode()) {
			super.insert(basedir, newentry);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.insert(basedir, newentry);
				}
			});
		}
	}

	@Override
	public void update(final DBPathEntry oldentry, final PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(oldentry != null);
		Assertion.assertNullPointerException(newentry != null);
		Assertion.assertAssertionError(oldentry.getPath().equals(newentry.getPath()),
				"!! old and new entry paths do not match:\nold=" + oldentry.getPath() + "\nnew=" + newentry.getPath());
		Assertion.assertAssertionError(oldentry.getType() == newentry.getType());

		if (! PathEntry.dscMatch(oldentry, newentry)
				|| oldentry.isCsumNull() != newentry.isCsumNull()
				|| (!oldentry.isCsumNull() && !newentry.isCsumNull() && oldentry.getCsum() != newentry.getCsum())
				|| oldentry.getStatus() != newentry.getStatus()
				) {
			if (isConsumeUpdateQueueMode()) {
				super.update(oldentry, newentry);
			} else {
				enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
					public void run() throws SQLException, InterruptedException {
						UpdaterWithUpdateQueue.super.update(oldentry, newentry);
					}
				});
			}
		}
	}

	@Override
	public void updateStatus(final DBPathEntry entry, final int newstatus)
			throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		if (isConsumeUpdateQueueMode()) {
			super.updateStatus(entry, newstatus);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.updateStatus(entry, newstatus);
				}
			});
		}
	}

	@Override
	public void delete(final DBPathEntry entry) throws SQLException, InterruptedException {
		if (isConsumeUpdateQueueMode()) {
			super.delete(entry);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.delete(entry);
				}
			});
		}
	}

	@Override
	protected void deleteLowPriority(final DBPathEntry entry) throws SQLException, InterruptedException {
		if (isConsumeUpdateQueueMode()) {
			super.deleteLowPriority(entry);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.deleteLowPriority(entry);
				}
			}, 1);
		}
	}

	@Override
	public void deleteChildren(final DBPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		if (isConsumeUpdateQueueMode()) {
			try {
				endConsumeUpdateQueueMode();
				// temporarily get out of UpdateQueueMode; delete()'s inside super.deleteChildre() are enqueued
				super.deleteChildren(entry);
			} finally {
				beginConsumeUpdateQueueMode();
			}
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.deleteChildren(entry);
				}
			});
		}
	}

	@Override
	public void unsetClean(long pathid) throws SQLException, InterruptedException {
		if (isConsumeUpdateQueueMode()) {
			super.unsetClean(pathid);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.unsetClean(pathid);
				}
			});
		}

	}

	@Override
	public void disable(final DBPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		if (isConsumeUpdateQueueMode()) {
			super.disable(entry);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.disable(entry);
				}
			});
		}
	}

	@Override
	public void disable(final DBPathEntry entry, final PathEntry newentry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertNullPointerException(newentry != null);
		if (isConsumeUpdateQueueMode()) {
			super.disable(entry, newentry);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.disable(entry, newentry);
				}
			});
		}
	}

	@Override
	public void updateParentId(final DBPathEntry entry, final long newparentid) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		if (isConsumeUpdateQueueMode()) {
			super.updateParentId(entry, newparentid);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.updateParentId(entry, newparentid);
				}
			});
		}
	}

	@Override
	public void orphanize(final DBPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		Assertion.assertAssertionError(entry.getParentId() != 0);
		if (isConsumeUpdateQueueMode()) {
			super.orphanize(entry);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.orphanize(entry);
				}
			});
		}
	}

	@Override
	public void orphanizeChildren(final DBPathEntry entry) throws SQLException, InterruptedException {
		Assertion.assertNullPointerException(entry != null);
		if (isConsumeUpdateQueueMode()) {
			try {
				endConsumeUpdateQueueMode();
				// temporarily get out of UpdateQueueMode; orphanize()'s inside super.orphanizeChildren() are enqueued
				super.orphanizeChildren(entry);
			} finally {
				beginConsumeUpdateQueueMode();
			}
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.orphanizeChildren(entry);
				}
			});
		}
	}

	public void noop() throws InterruptedException {
		enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
			public void run() throws SQLException, InterruptedException {
				// noop
			}
		});
	}

	@Override
	public void insertUpperLower(final long upper, final long lower, final int distance)
			throws SQLException, InterruptedException {
		if (isConsumeUpdateQueueMode()) {
			super.insertUpperLower(upper, lower, distance);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.insertUpperLower(upper, lower, distance);
				}
			});
		}
	}

	@Override
	public void deleteUpperLower(final long upper, final long lower) throws SQLException, InterruptedException {
		if (isConsumeUpdateQueueMode()) {
			super.deleteUpperLower(upper, lower);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.deleteUpperLower(upper, lower);
				}
			});
		}
	}

	@Override
	public void deleteUpperLower(final long pathid) throws SQLException, InterruptedException {
		if (isConsumeUpdateQueueMode()) {
			try {
				endConsumeUpdateQueueMode();
				// temporarily get out of UpdateQueueMode; delete()'s inside super.deleteUpperLower() are enqueued
				super.deleteUpperLower(pathid);
			} finally {
				beginConsumeUpdateQueueMode();
			}
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.deleteUpperLower(pathid);
				}
			});
		}
	}

	@Override
	public int refreshDirectUpperLower(Set<Long> dontListRootIds, IsEol isEol)
			throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! isConsumeUpdateQueueMode());
		return super.refreshDirectUpperLower(dontListRootIds, isEol);
	}

	@Override
	public int refreshIndirectUpperLower(Set<Long> dontListRootIds, IsEol isEol)
			throws SQLException, InterruptedException {
		Assertion.assertAssertionError(! isConsumeUpdateQueueMode());
		return super.refreshIndirectUpperLower(dontListRootIds, isEol);
	}

	@Override
	public void insertEquality(final long pathid1, final long pathid2, final long size, final int csum)
			throws SQLException, InterruptedException {
		if (isConsumeUpdateQueueMode()) {
			super.insertEquality(pathid1, pathid2, size, csum);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.insertEquality(pathid1, pathid2, size, csum);
				}
			});
		}
	}

	@Override
	public void deleteEquality(final long pathid1, final long pathid2) throws InterruptedException, SQLException {
		if (isConsumeUpdateQueueMode()) {
			super.deleteEquality(pathid1, pathid2);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.deleteEquality(pathid1, pathid2);
				}
			});
		}
	}

	@Override
	public void updateEquality(final long pathid1, final long pathid2) throws InterruptedException, SQLException {
		if (isConsumeUpdateQueueMode()) {
			super.updateEquality(pathid1, pathid2);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.updateEquality(pathid1, pathid2);
				}
			});
		}
	}

	@Override
	public void updateDuplicateFields(final long pathid, final long duplicate, final long dedupablesize)
			throws InterruptedException, SQLException {
		if (isConsumeUpdateQueueMode()) {
			super.updateDuplicateFields(pathid, duplicate, dedupablesize);
		} else {
			enqueueUpdate(new RunnableWithException2<SQLException, InterruptedException> () {
				public void run() throws SQLException, InterruptedException {
					UpdaterWithUpdateQueue.super.updateDuplicateFields(pathid, duplicate, dedupablesize);
				}
			});
		}
	}


}
