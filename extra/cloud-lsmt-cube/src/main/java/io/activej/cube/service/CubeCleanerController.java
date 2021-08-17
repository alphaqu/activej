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
import io.activej.cube.ot.CubeUplink;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static io.activej.async.function.AsyncSuppliers.reuse;
import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;

public final class CubeCleanerController<K, D, C> implements EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeCleanerController.class);

	public static final Duration DEFAULT_CHUNKS_CLEANUP_DELAY = Duration.ofMinutes(1);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;

	private final CubeUplink<K, D, ?> uplink;
	private final ActiveFsChunkStorage<C> chunksStorage;

	private Duration chunksCleanupDelay = DEFAULT_CHUNKS_CLEANUP_DELAY;

	private final PromiseStats promiseCleanup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupCollectRequiredChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupRepository = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeCleanerController(Eventloop eventloop,
			CubeUplink<K, D, ?> uplink,
			ActiveFsChunkStorage<C> chunksStorage) {
		this.eventloop = eventloop;
		this.uplink = uplink;
		this.chunksStorage = chunksStorage;
	}

	public static <K, D, C> CubeCleanerController<K, D, C> create(Eventloop eventloop,
			CubeUplink<K, D, ?> uplink,
			ActiveFsChunkStorage<C> storage) {
		return new CubeCleanerController<>(eventloop, uplink, storage);
	}

	public CubeCleanerController<K, D, C> withChunksCleanupDelay(Duration chunksCleanupDelay) {
		this.chunksCleanupDelay = chunksCleanupDelay;
		return this;
	}

	private final AsyncSupplier<Void> cleanup = reuse(this::doCleanup);

	public Promise<Void> cleanup() {
		return cleanup.get();
	}

	private Promise<Void> doCleanup() {
		Instant safePoint = eventloop.currentInstant().minus(chunksCleanupDelay);

		return uplink.<C>getRequiredChunks()
				.then(requiredChunks -> chunksStorage.checkRequiredChunks(requiredChunks)
						.then(() -> chunksStorage.cleanup(requiredChunks, safePoint)
								.whenComplete(promiseCleanupChunks.recordStats()))
						.whenComplete(logger.isTraceEnabled() ?
								toLogger(logger, TRACE, thisMethod(), safePoint, requiredChunks) :
								toLogger(logger, thisMethod(), safePoint, Utils.toString(requiredChunks))));
	}

	@JmxAttribute
	public Duration getChunksCleanupDelay() {
		return chunksCleanupDelay;
	}

	@JmxAttribute
	public void setChunksCleanupDelay(Duration chunksCleanupDelay) {
		this.chunksCleanupDelay = chunksCleanupDelay;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanup() {
		return promiseCleanup;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupCollectRequiredChunks() {
		return promiseCleanupCollectRequiredChunks;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupRepository() {
		return promiseCleanupRepository;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupChunks() {
		return promiseCleanupChunks;
	}

	@JmxOperation
	public void cleanupNow() {
		cleanup();
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}
