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

package io.activej.cube.ot;

import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.common.ApplicationSettings;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.tuple.Tuple2;
import io.activej.etl.LogDiff;
import io.activej.etl.LogPositionDiff;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;
import io.activej.ot.exception.OTException;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.transformMap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.util.Collections.*;
import static java.util.stream.Collectors.joining;

public final class CubeUplinkMySql implements CubeUplink<Long, LogDiff<CubeDiff>, CubeUplinkMySql.UplinkProtoCommit> {
	private static final Logger logger = LoggerFactory.getLogger(CubeUplinkMySql.class);

	public static final String DEFAULT_REVISION_TABLE = ApplicationSettings.getString(CubeUplinkMySql.class, "revisionTable", "revision");
	public static final String DEFAULT_POSITION_TABLE = ApplicationSettings.getString(CubeUplinkMySql.class, "positionTable", "position");
	public static final String DEFAULT_CHUNK_TABLE = ApplicationSettings.getString(CubeUplinkMySql.class, "chunkTable", "chunk");
	public static final String DEFAULT_POSITION_BACKUP_TABLE = ApplicationSettings.getString(CubeUplinkMySql.class, "positionBackupTable", null);
	public static final String DEFAULT_CHUNK_BACKUP_TABLE = ApplicationSettings.getString(CubeUplinkMySql.class, "chunkBackupTable", null);

	private static final MeasuresValidator NO_MEASURE_VALIDATION = ($1, $2) -> {};

	private final Executor executor;
	private final DataSource dataSource;

	private final PrimaryKeyCodecs primaryKeyCodecs;

	private MeasuresValidator measuresValidator = NO_MEASURE_VALIDATION;

	private String tableRevision = DEFAULT_REVISION_TABLE;
	private String tablePosition = DEFAULT_POSITION_TABLE;
	private String tableChunk = DEFAULT_CHUNK_TABLE;
	private String tablePositionBackup = DEFAULT_POSITION_BACKUP_TABLE;
	private String tableChunkBackup = DEFAULT_CHUNK_BACKUP_TABLE;

	private CubeUplinkMySql(Executor executor, DataSource dataSource, PrimaryKeyCodecs primaryKeyCodecs) {
		this.executor = executor;
		this.dataSource = dataSource;
		this.primaryKeyCodecs = primaryKeyCodecs;
	}

	public static CubeUplinkMySql create(Executor executor, DataSource dataSource, PrimaryKeyCodecs primaryKeyCodecs) {
		return new CubeUplinkMySql(executor, dataSource, primaryKeyCodecs);
	}

	public CubeUplinkMySql withCustomTableNames(String tableRevision, String tablePosition, String tableChunk) {
		this.tableRevision = tableRevision;
		this.tablePosition = tablePosition;
		this.tableChunk = tableChunk;
		return this;
	}

	public CubeUplinkMySql withCustomTableNames(String tableRevision, String tablePosition, String tableChunk, String tablePositionBackup, String tableChunkBackup) {
		this.tableRevision = tableRevision;
		this.tablePosition = tablePosition;
		this.tableChunk = tableChunk;
		this.tablePositionBackup = tablePositionBackup;
		this.tableChunkBackup = tableChunkBackup;
		return this;
	}

	public CubeUplinkMySql withMeasuresValidator(MeasuresValidator measuresValidator) {
		this.measuresValidator = measuresValidator;
		return this;
	}

	@Override
	public Promise<FetchData<Long, LogDiff<CubeDiff>>> checkout() {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);

						long revision;
						revision = getMaxRevision(connection);

						CubeDiff cubeDiff;
						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"SELECT `id`, `aggregation`, `measures`, `min_key`, `max_key`, `item_count` " +
								"FROM {chunk} " +
								"WHERE `removed_revision` IS NULL"
						))) {
							ResultSet resultSet = ps.executeQuery();

							Map<String, Set<AggregationChunk>> aggregationDiffs = new HashMap<>();
							while (resultSet.next()) {
								long chunkId = resultSet.getLong(1);
								String aggregationId = resultSet.getString(2);
								List<String> measures = measuresFromString(resultSet.getString(3));
								measuresValidator.validate(aggregationId, measures);
								PrimaryKey minKey = primaryKeyCodecs.fromString(aggregationId, resultSet.getString(4));
								PrimaryKey maxKey = primaryKeyCodecs.fromString(aggregationId, resultSet.getString(5));
								int count = resultSet.getInt(6);

								aggregationDiffs.computeIfAbsent(aggregationId, $ -> new HashSet<>())
										.add(AggregationChunk.create(chunkId, measures, minKey, maxKey, count));
							}

							cubeDiff = CubeDiff.of(transformMap(aggregationDiffs, AggregationDiff::of));
						}

						Map<String, LogPositionDiff> positions = new HashMap<>();
						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"SELECT p.`partition_id`, p.`filename`, p.`remainder`, p.`position` " +
								"FROM {position} p " +
								"INNER JOIN" +
								" (SELECT `partition_id`, MAX(`revision_id`) AS `max_revision`" +
								" FROM {position}" +
								" GROUP BY `partition_id`) g " +
								"ON p.`partition_id` = g.`partition_id` " +
								"AND p.`revision_id` = g.`max_revision`"
						))) {
							ResultSet resultSet = ps.executeQuery();
							while (resultSet.next()) {
								String partition = resultSet.getString(1);
								String filename = resultSet.getString(2);
								int remainder = resultSet.getInt(3);
								long position = resultSet.getLong(4);

								LogFile logFile = new LogFile(filename, remainder);
								LogPosition logPosition = LogPosition.create(logFile, position);
								LogPositionDiff logPositionDiff = new LogPositionDiff(LogPosition.initial(), logPosition);

								positions.put(partition, logPositionDiff);
							}
						}
						connection.commit();

						return new FetchData<>(revision, revision, toLogDiffs(cubeDiff, positions));
					}
				});
	}

	@Override
	public Promise<FetchData<Long, LogDiff<CubeDiff>>> fetch(Long currentCommitId) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);

						long revision = getMaxRevision(connection);
						if (revision == currentCommitId) return new FetchData<>(revision, revision, emptyList());
						if (revision < currentCommitId) {
							throw new IllegalArgumentException("Passed revision is higher than uplink revision");
						}

						FetchData<Long, LogDiff<CubeDiff>> result = new FetchData<>(revision, revision, toLogDiffs(
								fetchChunkDiffs(connection, currentCommitId, null),
								fetchPositionDiffs(connection, currentCommitId, null)
						));
						connection.commit();
						return result;
					}
				});
	}

	@Override
	public Promise<UplinkProtoCommit> createProtoCommit(Long parent, List<LogDiff<CubeDiff>> diffs, long parentLevel) {
		checkArgument(parent == parentLevel, "Level mismatch");

		return Promise.of(new UplinkProtoCommit(parent, diffs));
	}

	@Override
	public Promise<FetchData<Long, LogDiff<CubeDiff>>> push(UplinkProtoCommit protoCommit) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);

						long newRevision;
						while (true) {
							long revision = getMaxRevision(connection);
							if (revision < protoCommit.parentRevision) {
								throw new IllegalArgumentException("Uplink revision is less than parent revision");
							}

							newRevision = revision + 1;
							try (PreparedStatement ps = connection.prepareStatement(sql("" +
									"INSERT INTO {revision} (`revision`) VALUE (?)"
							))) {
								ps.setLong(1, newRevision);
								ps.executeUpdate();
								break;
							} catch (SQLIntegrityConstraintViolationException ignored) {
								// someone pushed to the same revision number, retry
							}
						}

						List<LogDiff<CubeDiff>> diffsList = protoCommit.diffs;

						Set<ChunkWithAggregationId> added = collectChunks(diffsList, true);
						Set<ChunkWithAggregationId> removed = collectChunks(diffsList, false);

						// squash
						added.removeAll(removed);
						removed.removeAll(added);

						if (!added.isEmpty()) {
							addChunks(connection, newRevision, added);
						}

						if (!removed.isEmpty()) {
							removeChunks(connection, newRevision, removed);
						}

						Map<String, LogPosition> positions = collectPositions(diffsList);
						if (!positions.isEmpty()) {
							updatePositions(connection, newRevision, positions);
						}

						if (newRevision == protoCommit.parentRevision + 1) {
							connection.commit();

							return new FetchData<>(newRevision, newRevision, emptyList());
						}

						FetchData<Long, LogDiff<CubeDiff>> result = new FetchData<>(newRevision, newRevision, toLogDiffs(
								fetchChunkDiffs(connection, protoCommit.parentRevision, newRevision - 1),
								fetchPositionDiffs(connection, protoCommit.parentRevision, newRevision - 1)
						));
						connection.commit();
						return result;
					}
				});
	}

	@Override
	@SuppressWarnings("unchecked")
	public Promise<Set<Long>> getRequiredChunks(Instant safePoint) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"SELECT c.`id` " +
								"FROM {chunk} c" +
								" INNER JOIN {revision} r" +
								" ON r.revision=c.added_revision " +
								"WHERE c.`removed_revision` IS NULL OR r.`added_at`>?"
						))) {
							ps.setTimestamp(1, Timestamp.from(safePoint));

							ResultSet resultSet = ps.executeQuery();

							Set<Long> requiredChunks = new HashSet<>();
							while (resultSet.next()) {
								requiredChunks.add(resultSet.getLong(1));
							}
							return requiredChunks;
						}
					}
				});
	}

	@Override
	public Promise<Void> backup(Long revisionId) {
		boolean backupPositions = tablePositionBackup != null;
		boolean backupChunks = tableChunkBackup != null;
		checkState(backupPositions || backupChunks, "Both backup tables are not set");

		boolean transactionNeeded = backupPositions && backupChunks;

		return Promise.ofBlockingRunnable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						if (transactionNeeded) connection.setAutoCommit(false);

						connection.setTransactionIsolation(TRANSACTION_READ_UNCOMMITTED);

						if (backupPositions) backupPositions(connection, revisionId);
						if (backupChunks) backupChunks(connection, revisionId);

						if (transactionNeeded) connection.commit();
					}
				});
	}

	private CubeDiff fetchChunkDiffs(Connection connection, long from, @Nullable Long to) throws SQLException, MalformedDataException {
		CubeDiff cubeDiff;
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"SELECT `id`, `aggregation`, `measures`, `min_key`, `max_key`, `item_count`, ISNULL(`removed_revision`) " +
				"FROM {chunk} " +
				"WHERE" +
				" (`added_revision`<=? AND `removed_revision`>?" + (to == null ? "" : " AND `removed_revision`<=?") + ")" +
				" OR" +
				" (`added_revision`>? AND `removed_revision` IS NULL " + (to == null ? "" : " AND `added_revision`<=?") + ")"
		))) {
			if (to == null) {
				for (int i = 1; i <= 3; i++) {
					ps.setLong(i, from);
				}
			} else {
				ps.setLong(1, from);
				ps.setLong(2, from);
				ps.setLong(3, to);
				ps.setLong(4, from);
				ps.setLong(5, to);
			}
			ResultSet resultSet = ps.executeQuery();

			Map<String, Tuple2<Set<AggregationChunk>, Set<AggregationChunk>>> aggregationDiffs = new HashMap<>();
			while (resultSet.next()) {
				long chunkId = resultSet.getLong(1);
				String aggregationId = resultSet.getString(2);
				List<String> measures = measuresFromString(resultSet.getString(3));
				measuresValidator.validate(aggregationId, measures);
				PrimaryKey minKey = primaryKeyCodecs.fromString(aggregationId, resultSet.getString(4));
				PrimaryKey maxKey = primaryKeyCodecs.fromString(aggregationId, resultSet.getString(5));
				int count = resultSet.getInt(6);
				boolean isAdded = resultSet.getBoolean(7);

				AggregationChunk chunk = AggregationChunk.create(chunkId, measures, minKey, maxKey, count);

				Tuple2<Set<AggregationChunk>, Set<AggregationChunk>> tuple = aggregationDiffs
						.computeIfAbsent(aggregationId, $ -> new Tuple2<>(new HashSet<>(), new HashSet<>()));

				if (isAdded) {
					tuple.getValue1().add(chunk);
				} else {
					tuple.getValue2().add(chunk);
				}
			}

			cubeDiff = CubeDiff.of(transformMap(aggregationDiffs, tuple -> AggregationDiff.of(tuple.getValue1(), tuple.getValue2())));
		}
		return cubeDiff;
	}

	private Map<String, LogPositionDiff> fetchPositionDiffs(Connection connection, long from, @Nullable Long to) throws SQLException {
		Map<String, LogPositionDiff> positions = new HashMap<>();
		// TODO: maybe filter out on DB side ?
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"SELECT p.`partition_id`, p.`filename`, p.`remainder`, p.`position`, g.`to` " +
				"FROM {position} p" +
				" INNER JOIN" +
				" (SELECT `partition_id`, MAX(`revision_id`) AS `max_revision`, IF(`revision_id`>?, TRUE, FALSE) as `to`" +
				" FROM {position}" +
				(to == null ? "" : " WHERE `revision_id`<=?") +
				" GROUP BY `partition_id`, `to`) g" +
				" ON p.`partition_id` = g.`partition_id`" +
				" AND p.`revision_id` = g.`max_revision`" +
				"ORDER BY p.`partition_id`, `to`"
		))) {
			ps.setLong(1, from);
			if (to != null) ps.setLong(2, to);

			ResultSet resultSet = ps.executeQuery();
			LogPosition[] fromTo = new LogPosition[2];
			String currentPartition = null;
			while (resultSet.next()) {
				String partition = resultSet.getString(1);
				if (!partition.equals(currentPartition)) {
					fromTo[0] = null;
					fromTo[1] = null;
				}
				currentPartition = partition;
				String filename = resultSet.getString(2);
				int remainder = resultSet.getInt(3);
				long position = resultSet.getLong(4);
				boolean isTo = resultSet.getBoolean(5);

				LogFile logFile = new LogFile(filename, remainder);
				LogPosition logPosition = LogPosition.create(logFile, position);

				if (isTo) {
					if (fromTo[0] == null) {
						fromTo[0] = LogPosition.initial();
					}
					fromTo[1] = logPosition;
				} else {
					fromTo[0] = logPosition;
					fromTo[1] = null;
					continue;
				}

				LogPositionDiff logPositionDiff = new LogPositionDiff(fromTo[0], fromTo[1]);
				positions.put(partition, logPositionDiff);
			}
		}
		return positions;
	}

	private void backupChunks(Connection connection, Long revisionId) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"INSERT INTO {chunk_backup} " +

				"SELECT ?, `id`, `aggregation`, `measures`, `min_key`, `max_key`, `item_count`, `added_revision`," +
				" IF(`removed_revision`<=?, `removed_revision`, NULL), `last_modified_at` " +

				"FROM {chunk}" +
				"WHERE `added_revision`<=?"
		))) {
			ps.setLong(1, revisionId);
			ps.setLong(2, revisionId);
			ps.setLong(3, revisionId);

			ps.executeUpdate();
		}
	}

	private void backupPositions(Connection connection, Long revisionId) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"INSERT INTO {position_backup} " +
				"SELECT ?, {position}.* " +
				"FROM {position} " +
				"WHERE {position}.`revision_id`<=?"
		))) {
			ps.setLong(1, revisionId);
			ps.setLong(2, revisionId);

			ps.executeUpdate();
		}
	}

	private void addChunks(Connection connection, long newRevision, Set<ChunkWithAggregationId> chunks) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"INSERT INTO {chunk} (`id`, `aggregation`, `measures`, `min_key`, `max_key`, `item_count`, `added_revision`) " +
				"VALUES " + String.join(",", nCopies(chunks.size(), "(?,?,?,?,?,?,?)"))
		))) {
			int index = 1;
			for (ChunkWithAggregationId chunk : chunks) {
				String aggregationId = chunk.aggregationId;
				AggregationChunk aggregationChunk = chunk.chunk;

				ps.setLong(index++, (long) aggregationChunk.getChunkId());
				ps.setString(index++, aggregationId);
				List<String> measures = aggregationChunk.getMeasures();
				measuresValidator.validate(aggregationId, measures);
				ps.setString(index++, measuresToString(measures));
				ps.setString(index++, primaryKeyCodecs.toString(aggregationId, aggregationChunk.getMinPrimaryKey()));
				ps.setString(index++, primaryKeyCodecs.toString(aggregationId, aggregationChunk.getMaxPrimaryKey()));
				ps.setInt(index++, aggregationChunk.getCount());
				ps.setLong(index++, newRevision);
			}

			ps.executeUpdate();
		} catch (MalformedDataException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private void removeChunks(Connection connection, long newRevision, Set<ChunkWithAggregationId> chunks) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"UPDATE {chunk} " +
				"SET `removed_revision`=? " +
				"WHERE `id` IN " +
				nCopies(chunks.size(), "?")
						.stream()
						.collect(joining(",", "(", ")"))
		))) {
			int index = 1;
			ps.setLong(index++, newRevision);
			for (ChunkWithAggregationId chunk : chunks) {
				ps.setLong(index++, (Long) chunk.chunk.getChunkId());
			}

			ps.executeUpdate();
		}
	}

	private void updatePositions(Connection connection, long newRevision, Map<String, LogPosition> positions) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"INSERT INTO {position} (`revision_id`, `partition_id`, `filename`, `remainder`, `position`) " +
				"VALUES " + String.join(",", nCopies(positions.size(), "(?,?,?,?,?)"))
		))) {
			int index = 1;
			for (Map.Entry<String, LogPosition> entry : positions.entrySet()) {
				LogPosition position = entry.getValue();
				LogFile logFile = position.getLogFile();

				ps.setLong(index++, newRevision);
				ps.setString(index++, entry.getKey());
				ps.setString(index++, logFile.getName());
				ps.setInt(index++, logFile.getRemainder());
				ps.setLong(index++, position.getPosition());
			}

			ps.executeUpdate();
		}
	}

	private List<LogDiff<CubeDiff>> toLogDiffs(CubeDiff cubeDiff, Map<String, LogPositionDiff> positions) {
		List<LogDiff<CubeDiff>> logDiffs;
		if (cubeDiff.isEmpty()) {
			if (positions.isEmpty()) {
				logDiffs = emptyList();
			} else {
				logDiffs = singletonList(LogDiff.of(positions, emptyList()));
			}
		} else {
			logDiffs = singletonList(LogDiff.of(positions, cubeDiff));
		}
		return logDiffs;
	}

	private String sql(String sql) {
		return sql
				.replace("{revision}", tableRevision)
				.replace("{position}", tablePosition)
				.replace("{chunk}", tableChunk)
				.replace("{position_backup}", Objects.toString(tablePositionBackup))
				.replace("{chunk_backup}", Objects.toString(tableChunkBackup));
	}

	public void initialize() throws IOException, SQLException {
		logger.trace("Initializing tables");
		execute(dataSource, sql(new String(loadResource("sql/uplink_revision.sql"), UTF_8)));
		execute(dataSource, sql(new String(loadResource("sql/uplink_chunk.sql"), UTF_8)));
		execute(dataSource, sql(new String(loadResource("sql/uplink_position.sql"), UTF_8)));
		if (tablePositionBackup != null) {
			execute(dataSource, sql(new String(loadResource("sql/uplink_position_backup.sql"), UTF_8)));
		}
		if (tableChunkBackup != null) {
			execute(dataSource, sql(new String(loadResource("sql/uplink_chunk_backup.sql"), UTF_8)));
		}
	}

	private static byte[] loadResource(String name) throws IOException {
		try (InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
			assert stream != null;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			int size;
			while ((size = stream.read(buffer)) != -1) {
				baos.write(buffer, 0, size);
			}
			return baos.toByteArray();
		}
	}

	private static void execute(DataSource dataSource, String sql) throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql);
			}
		}
	}

	public void truncateTables() throws SQLException {
		logger.trace("Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql("DELETE FROM {revision} WHERE `revision` != 0"));
			}
		}
	}

	private static List<String> measuresFromString(String measuresString) {
		return Arrays.stream(measuresString.split(" ")).collect(Collectors.toList());
	}

	private static String measuresToString(List<String> measures) {
		return String.join(" ", measures);
	}

	private long getMaxRevision(Connection connection) throws SQLException, OTException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"SELECT MAX(`revision`) FROM {revision}"
		))) {
			ResultSet resultSet = ps.executeQuery();
			if (!resultSet.next()) {
				throw new OTException("Empty repository");
			}
			return resultSet.getLong(1);
		}
	}

	private static Set<ChunkWithAggregationId> collectChunks(List<LogDiff<CubeDiff>> diffsList, boolean added) {
		Set<ChunkWithAggregationId> chunks = new HashSet<>();
		for (LogDiff<CubeDiff> logDiff : diffsList) {
			for (CubeDiff cubeDiff : logDiff.getDiffs()) {
				for (Map.Entry<String, AggregationDiff> entry : cubeDiff.entrySet()) {
					String aggregationId = entry.getKey();
					AggregationDiff diff = entry.getValue();
					for (AggregationChunk chunk : added ? diff.getAddedChunks() : diff.getRemovedChunks()) {
						chunks.add(new ChunkWithAggregationId(chunk, aggregationId));
					}
				}
			}
		}
		return chunks;
	}

	private static Map<String, LogPosition> collectPositions(List<LogDiff<CubeDiff>> diffsList) {
		Map<String, LogPosition> result = new HashMap<>();
		for (LogDiff<CubeDiff> logDiff : diffsList) {
			for (Map.Entry<String, LogPositionDiff> entry : logDiff.getPositions().entrySet()) {
				result.put(entry.getKey(), entry.getValue().to);
			}
		}
		return result;
	}

	public static final class UplinkProtoCommit {
		private final long parentRevision;
		private final List<LogDiff<CubeDiff>> diffs;

		public UplinkProtoCommit(long parentRevision, List<LogDiff<CubeDiff>> diffs) {
			this.parentRevision = parentRevision;
			this.diffs = diffs;
		}

		public long getParentRevision() {
			return parentRevision;
		}

		public List<LogDiff<CubeDiff>> getDiffs() {
			return diffs;
		}
	}

	private static class ChunkWithAggregationId {
		private final AggregationChunk chunk;
		private final String aggregationId;

		private ChunkWithAggregationId(AggregationChunk chunk, String aggregationId) {
			this.chunk = chunk;
			this.aggregationId = aggregationId;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ChunkWithAggregationId that = (ChunkWithAggregationId) o;
			return chunk.equals(that.chunk);
		}

		@Override
		public int hashCode() {
			return Objects.hash(chunk);
		}
	}
}
