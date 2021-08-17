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
import io.activej.common.ApplicationSettings;
import io.activej.common.Utils;
import io.activej.cube.exception.CubeException;
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

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

import static io.activej.aggregation.util.Utils.wrapException;
import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.cube.Utils.executeSqlScript;
import static io.activej.cube.Utils.loadResource;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class CubeBackupController implements EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeBackupController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	public static final String REVISION_TABLE = ApplicationSettings.getString(CubeBackupController.class, "revisionTable", "revision");
	public static final String POSITION_TABLE = ApplicationSettings.getString(CubeBackupController.class, "positionTable", "position");
	public static final String CHUNK_TABLE = ApplicationSettings.getString(CubeBackupController.class, "chunkTable", "chunk");

	public static final String BACKUP_TABLE = ApplicationSettings.getString(CubeBackupController.class, "backupTable", "backup");
	public static final String BACKUP_POSITION_TABLE = ApplicationSettings.getString(CubeBackupController.class, "backupPositionTable", "backup_position");
	public static final String BACKUP_CHUNK_TABLE = ApplicationSettings.getString(CubeBackupController.class, "backupChunkTable", "backup_chunk");

	private final Eventloop eventloop;
	private final Executor executor;
	private final DataSource dataSource;
	private final ActiveFsChunkStorage<Long> storage;

	private final PromiseStats promiseBackup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupDb = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseFinishBackupDb = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	private CubeBackupController(Eventloop eventloop,
			Executor executor,
			DataSource dataSource,
			ActiveFsChunkStorage<Long> storage) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.dataSource = dataSource;
		this.storage = storage;
	}

	public static CubeBackupController create(Eventloop eventloop,
			Executor executor,
			DataSource dataSource,
			ActiveFsChunkStorage<Long> storage) {
		return new CubeBackupController(eventloop, executor, dataSource, storage);
	}

	@SuppressWarnings({"UnusedReturnValue", "Convert2MethodRef"})
	public Promise<Void> backup() {
		return getMaxRevisionId()
				.then(revisionId -> backup(revisionId));
	}

	public Promise<Void> backup(long revisionId) {
		return Promises.sequence(
						() -> backupDb(revisionId),
						() -> backupChunks(revisionId),
						() -> finishBackupDb(revisionId))
				.whenComplete(promiseBackup.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	private Promise<Void> backupChunks(long revisionId) {
		return getChunksToBackUp(revisionId)
				.then(chunkIds -> storage.backup(String.valueOf(revisionId), chunkIds)
						.thenEx(wrapException(e -> new CubeException("Failed to backup chunks on storage: " + storage, e)))
						.whenComplete(promiseBackupChunks.recordStats())
						.whenComplete(logger.isTraceEnabled() ?
								toLogger(logger, TRACE, thisMethod(), revisionId, chunkIds) :
								toLogger(logger, thisMethod(), revisionId, Utils.toString(chunkIds))));
	}

	private Promise<Void> backupDb(long revisionId) {
		return Promise.ofBlockingRunnable(executor, () -> {
					try {
						String sql = sql(new String(loadResource("sql/backup.sql"), UTF_8))
								.replace("{backup_revision}", String.valueOf(revisionId));

						executeSqlScript(dataSource, sql);
					} catch (SQLException e) {
						throw new CubeException("Failed to back up database", e);
					}
				})
				.whenComplete(promiseBackupDb.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId));
	}

	private Promise<Void> finishBackupDb(long revisionId) {
		return Promise.ofBlockingRunnable(executor, () -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement stmt = connection.prepareStatement(sql("" +
								"UPDATE {backup} " +
								"SET `backup_finished_at` = NOW() " +
								"WHERE `revision` = ?"))) {
							stmt.setLong(1, revisionId);

							stmt.executeUpdate();
						}
					} catch (SQLException e) {
						throw new CubeException("Failed to finish back up", e);
					}
				})
				.whenComplete(promiseFinishBackupDb.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId));
	}

	private Promise<Long> getMaxRevisionId() {
		return Promise.ofBlockingCallable(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (Statement statement = connection.createStatement()) {
					ResultSet resultSet = statement.executeQuery(sql("SELECT MAX(`revision`) FROM {revision}"));

					if (!resultSet.next()) {
						throw new CubeException("Cube is not initialized");
					}
					return resultSet.getLong(1);
				}
			} catch (SQLException e) {
				throw new CubeException("Failed to retrieve maximum revision ID", e);
			}
		});
	}

	private Promise<Set<Long>> getChunksToBackUp(long revisionId) {
		return Promise.ofBlockingCallable(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement stmt = connection.prepareStatement(sql("" +
						"SELECT `id` " +
						"FROM {backup_chunk} " +
						"WHERE `backup_id` = ?"))) {
					stmt.setLong(1, revisionId);

					ResultSet resultSet = stmt.executeQuery();

					Set<Long> chunkIds = new HashSet<>();
					while (resultSet.next()) {
						chunkIds.add(resultSet.getLong(1));
					}
					return chunkIds;
				}
			} catch (SQLException e) {
				throw new CubeException("Failed to retrieve chunks to back up", e);
			}
		});
	}

	private static String sql(String sql) {
		return sql
				.replace("{revision}", REVISION_TABLE)
				.replace("{position}", POSITION_TABLE)
				.replace("{chunk}", CHUNK_TABLE)
				.replace("{backup}", BACKUP_TABLE)
				.replace("{backup_position}", BACKUP_POSITION_TABLE)
				.replace("{backup_chunk}", BACKUP_CHUNK_TABLE);
	}

	public void initialize() throws IOException, SQLException {
		logger.trace("Initializing tables");
		executeSqlScript(dataSource, sql(new String(loadResource("sql/ddl/uplink_revision.sql"), UTF_8)));
		executeSqlScript(dataSource, sql(new String(loadResource("sql/ddl/uplink_chunk.sql"), UTF_8)));
		executeSqlScript(dataSource, sql(new String(loadResource("sql/ddl/uplink_position.sql"), UTF_8)));
		executeSqlScript(dataSource, sql(new String(loadResource("sql/ddl/uplink_backup.sql"), UTF_8)));
	}

	public void truncateTables() throws SQLException {
		logger.trace("Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql("TRUNCATE TABLE {chunk}"));
				statement.execute(sql("TRUNCATE TABLE {position}"));
				statement.execute(sql("DELETE FROM {revision} WHERE `revision`!=0"));

				statement.execute(sql("TRUNCATE TABLE {backup}"));
				statement.execute(sql("TRUNCATE TABLE {backup_chunk}"));
				statement.execute(sql("TRUNCATE TABLE {backup_position}"));
			}
		}
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

	@JmxAttribute
	public PromiseStats getPromiseFinishBackupDb() {
		return promiseFinishBackupDb;
	}
}

