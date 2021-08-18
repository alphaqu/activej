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
import io.activej.cube.exception.CubeException;
import io.activej.eventloop.Eventloop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.activej.cube.Utils.executeSqlScript;
import static io.activej.cube.Utils.loadResource;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class CubeBackupController {
	private static final Logger logger = LoggerFactory.getLogger(CubeBackupController.class);

	public static final String REVISION_TABLE = ApplicationSettings.getString(CubeBackupController.class, "revisionTable", "revision");
	public static final String POSITION_TABLE = ApplicationSettings.getString(CubeBackupController.class, "positionTable", "position");
	public static final String CHUNK_TABLE = ApplicationSettings.getString(CubeBackupController.class, "chunkTable", "chunk");

	public static final String BACKUP_TABLE = ApplicationSettings.getString(CubeBackupController.class, "backupTable", "backup");
	public static final String BACKUP_POSITION_TABLE = ApplicationSettings.getString(CubeBackupController.class, "backupPositionTable", "backup_position");
	public static final String BACKUP_CHUNK_TABLE = ApplicationSettings.getString(CubeBackupController.class, "backupChunkTable", "backup_chunk");

	private final Eventloop eventloop;
	private final DataSource dataSource;
	private final ActiveFsChunkStorage<Long> storage;

	private CubeBackupController(Eventloop eventloop,
			DataSource dataSource,
			ActiveFsChunkStorage<Long> storage) {
		this.eventloop = eventloop;
		this.dataSource = dataSource;
		this.storage = storage;
	}

	public static CubeBackupController create(Eventloop eventloop,
			DataSource dataSource,
			ActiveFsChunkStorage<Long> storage) {
		return new CubeBackupController(eventloop, dataSource, storage);
	}

	public void backup() throws CubeException {
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(false);
			long maxRevisionId = getMaxRevisionId(connection);
			doBackup(connection, maxRevisionId);
		} catch (SQLException e) {
			throw new CubeException("Failed to connect to the database", e);
		}
	}

	public void backup(long revisionId) throws CubeException {
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(false);
			doBackup(connection, revisionId);
		} catch (SQLException e) {
			throw new CubeException("Failed to connect to the database", e);
		}
	}

	private void doBackup(Connection connection, long revisionId) throws CubeException {
		backupDb(connection, revisionId);
		Set<Long> chunkIds = getChunksToBackup(connection, revisionId);
		backupChunks(chunkIds, revisionId);

		try {
			connection.commit();
		} catch (SQLException e) {
			throw new CubeException("Failed to commit backup transaction", e);
		}
	}

	private void backupChunks(Set<Long> chunkIds, long revisionId) throws CubeException {
		logger.trace("Backing up chunks {} on revision {}", chunkIds, revisionId);

		try {
			eventloop.submit(() -> storage.backup(String.valueOf(revisionId), chunkIds)).get();
		} catch (InterruptedException e) {
			throw new CubeException("Eventloop thread was interrupted", e);
		} catch (ExecutionException e) {
			throw new CubeException("Failed to backup chunks on storage ", e.getCause());
		}

		logger.trace("Chunks {} are backed up on revision {}", chunkIds, revisionId);
	}

	private void backupDb(Connection connection, long revisionId) throws CubeException {
		logger.trace("Backing up database on revision {}", revisionId);

		try (Statement statement = connection.createStatement()) {
			String backupScript = sql(new String(loadResource("sql/backup.sql"), UTF_8))
					.replace("{backup_revision}", String.valueOf(revisionId));
			statement.execute(backupScript);
		} catch (SQLException | IOException e) {
			throw new CubeException("Failed to back up database", e);
		}

		logger.trace("Database is backed up on revision {} " +
				"Waiting for chunks to back up prior to commit", revisionId);
	}

	private long getMaxRevisionId(Connection connection) throws CubeException {
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery(sql("SELECT MAX(`revision`) FROM {revision}"));

			if (!resultSet.next()) {
				throw new CubeException("Cube is not initialized");
			}
			return resultSet.getLong(1);
		} catch (SQLException e) {
			throw new CubeException("Failed to retrieve maximum revision ID", e);
		}
	}

	private Set<Long> getChunksToBackup(Connection connection, long revisionId) throws CubeException {
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
		} catch (SQLException e) {
			throw new CubeException("Failed to retrieve chunks to back up", e);
		}
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
}

