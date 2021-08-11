/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.cube.service;

import io.activej.aggregation.ActiveFsChunkStorage;
import io.activej.async.function.AsyncSupplier;
import io.activej.common.Utils;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.cube.ot.CubeUplink;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;

import static io.activej.aggregation.util.Utils.wrapException;
import static io.activej.async.function.AsyncSuppliers.reuse;
import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.cube.Utils.chunksInDiffs;

public final class CubeBackupController<K, D, C> implements EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeBackupController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final CubeUplink<K, D, ?> uplink;
	private final ActiveFsChunkStorage<C> storage;

	private final CubeDiffScheme<D> cubeDiffScheme;

	private final PromiseStats promiseBackup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupDb = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeBackupController(Eventloop eventloop,
			CubeUplink<K, D, ?> uplink,
			ActiveFsChunkStorage<C> storage, CubeDiffScheme<D> cubeDiffScheme) {
		this.eventloop = eventloop;
		this.uplink = uplink;
		this.storage = storage;
		this.cubeDiffScheme = cubeDiffScheme;
	}

	public static <K, D, C> CubeBackupController<K, D, C> create(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			CubeUplink<K, D, ?> uplink,
			ActiveFsChunkStorage<C> storage) {
		return new CubeBackupController<>(eventloop, uplink, storage, cubeDiffScheme);
	}

	private final AsyncSupplier<Void> backup = reuse(this::doBackup);

	@SuppressWarnings("UnusedReturnValue")
	public Promise<Void> backup() {
		return backup.get();
	}

	public Promise<Void> doBackup() {
		return uplink.checkout()
				.thenEx(wrapException(e -> new CubeException("Failed to check out", e)))
				.then(fetchData -> Promises.sequence(
						() -> backupChunks(fetchData.getCommitId(), chunksInDiffs(cubeDiffScheme, fetchData.getDiffs())),
						() -> backupDb(fetchData.getCommitId())))
				.whenComplete(promiseBackup.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	private Promise<Void> backupChunks(K commitId, Set<C> chunkIds) {
		return storage.backup(String.valueOf(commitId), chunkIds)
				.thenEx(wrapException(e -> new CubeException("Failed to backup chunks on storage: " + storage, e)))
				.whenComplete(promiseBackupChunks.recordStats())
				.whenComplete(logger.isTraceEnabled() ?
						toLogger(logger, TRACE, thisMethod(), commitId, chunkIds) :
						toLogger(logger, thisMethod(), commitId, Utils.toString(chunkIds)));
	}

	private Promise<Void> backupDb(K revisionId) {
		return uplink.backup(revisionId)
				.thenEx(wrapException(e -> new CubeException("Failed to backup chunks in repository: " + uplink, e)))
				.whenComplete(promiseBackupDb.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId));
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxOperation
	public void backupNow() {
		backup();
	}

	@JmxAttribute
	public PromiseStats getPromiseBackup() {
		return promiseBackup;
	}

	@JmxAttribute
	public PromiseStats getPromiseBackupDb() {
		return promiseBackupDb;
	}

	@JmxAttribute
	public PromiseStats getPromiseBackupChunks() {
		return promiseBackupChunks;
	}

}

