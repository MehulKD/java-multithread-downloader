package com.zhandui.download;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DownloadThreadPool extends ThreadPoolExecutor {

	private ConcurrentHashMap<Future<?>, Runnable> mRunnableMonitorHashMap = new ConcurrentHashMap<Future<?>, Runnable>();
	private ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Future<?>>> mMissionsMonitor = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Future<?>>>();

	public DownloadThreadPool(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				handler);
	}

	public DownloadThreadPool(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
	}

	public DownloadThreadPool(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory);
	}

	public DownloadThreadPool(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
			RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory, handler);
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		if (t == null) {
			System.out.println(Thread.currentThread().getId()
					+ " has been succeesfully finished!");
		} else {
			System.out.println(Thread.currentThread().getId()
					+ " errroed! Retry");
		}
		for (Future<?> future : mRunnableMonitorHashMap.keySet()) {
			if (future.isDone() == false) {
				DownloadRunnable runnable = (DownloadRunnable) mRunnableMonitorHashMap
						.get(future);
				DownloadRunnable newRunnable = runnable.split();
				if (newRunnable != null) {
					submit(newRunnable);
					break;
				}
			}
		}
	}

	@Override
	public Future<?> submit(Runnable task) {
		Future<?> future = super.submit(task);

		if (task instanceof DownloadRunnable) {
			DownloadRunnable runnable = (DownloadRunnable) task;

			if (mMissionsMonitor.containsKey(runnable.MISSION_ID)) {
				mMissionsMonitor.get(runnable.MISSION_ID).add(future);
			} else {
				ConcurrentLinkedQueue<Future<?>> queue = new ConcurrentLinkedQueue<Future<?>>();
				queue.add(future);
				mMissionsMonitor.put(runnable.MISSION_ID, queue);
			}

			mRunnableMonitorHashMap.put(future, task);

		} else {
			throw new RuntimeException(
					"runnable is not an instance of DownloadRunnable!");
		}
		return future;
	}

	public boolean isFinished(int missionId) {
		ConcurrentLinkedQueue<Future<?>> futures = mMissionsMonitor
				.get(missionId);
		if (futures == null)
			return true;

		for (Future<?> future : futures) {
			if (!future.isDone()) {
				return false;
			}
		}
		return true;
	}

	public void pause(int missionId) {
		ConcurrentLinkedQueue<Future<?>> futures = mMissionsMonitor
				.get(missionId);
		for (Future<?> future : futures) {
			future.cancel(true);
		}
	}

	public void cancel(int missionId) {
		ConcurrentLinkedQueue<Future<?>> futures = mMissionsMonitor
				.remove(missionId);
		for (Future<?> future : futures) {
			mRunnableMonitorHashMap.remove(future);
			future.cancel(true);
		}
	}
}
