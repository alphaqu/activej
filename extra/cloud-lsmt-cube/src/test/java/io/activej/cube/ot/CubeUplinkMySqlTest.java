package io.activej.cube.ot;

import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.util.StringCodec;
import io.activej.etl.LogDiff;
import io.activej.etl.LogPositionDiff;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;
import io.activej.ot.OTCommitFactory.DiffsWithLevel;
import io.activej.ot.OTState;
import io.activej.ot.exception.OTException;
import io.activej.ot.uplink.OTUplink.FetchData;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.common.Utils.mapOf;
import static io.activej.common.Utils.setOf;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.dataSource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CubeUplinkMySqlTest {
	public static final Random RANDOM = ThreadLocalRandom.current();

	private static final List<String> MEASURES = Arrays.asList("a", "b", "c", "d");
	private static final PrimaryKey MIN_KEY = PrimaryKey.ofArray("100", "200");
	private static final PrimaryKey MAX_KEY = PrimaryKey.ofArray("300", "400");
	private static final int COUNT = 12345;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private CubeUplinkMySql uplink;

	@Before
	public void setUp() throws Exception {
		DataSource dataSource = dataSource("test.properties");

		StringCodec<PrimaryKey> codec = StringCodec.of(
				value -> PrimaryKey.ofArray((Object[]) value.split(" ")),
				primaryKey -> primaryKey.values().stream()
						.map(Object::toString)
						.collect(joining(" "))
		);
		PrimaryKeyCodecs codecs = PrimaryKeyCodecs.ofLookUp($ -> codec);
		uplink = CubeUplinkMySql.create(Executors.newCachedThreadPool(), dataSource, codecs);

		uplink.initialize();
		uplink.truncateTables();
	}

	@Test
	public void checkoutEmpty() {
		FetchData<Long, LogDiff<CubeDiff>> result = await(uplink.checkout());

		assertEquals(0, result.getLevel());
		assertEquals(0, (long) result.getCommitId());
		assertTrue(result.getDiffs().isEmpty());
	}

	@Test
	public void fetchEmpty() {
		FetchData<Long, LogDiff<CubeDiff>> result = await(uplink.fetch(0L));

		assertEquals(0, result.getLevel());
		assertEquals(0, (long) result.getCommitId());
		assertTrue(result.getDiffs().isEmpty());
	}

	@Test
	public void createProtoCommit() {
		List<LogDiff<CubeDiff>> diffs = randomDiffs();
		DiffsWithLevel<LogDiff<CubeDiff>> protoCommit = await(uplink.createProtoCommit(0L, diffs, 0));

		assertEquals(0, protoCommit.getLevel());
		assertEquals(diffs, protoCommit.getDiffs());
	}

	@Test
	public void push() {
		List<LogDiff<CubeDiff>> diffs = randomDiffs();

		DiffsWithLevel<LogDiff<CubeDiff>> protoCommit = await(uplink.createProtoCommit(0L, diffs, 0));
		await(uplink.push(protoCommit));

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.checkout());

		assertEquals(1L, (long) fetchData.getCommitId());
		assertEquals(1L, fetchData.getLevel());
		assertDiffs(diffs, fetchData.getDiffs());
	}

	@Test
	public void fetch() {
		List<LogDiff<CubeDiff>> totalDiffs = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			List<LogDiff<CubeDiff>> diffs = randomDiffs();
			DiffsWithLevel<LogDiff<CubeDiff>> protoCommit = await(uplink.createProtoCommit((long) i, diffs, i));
			await(uplink.push(protoCommit));

			FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.fetch((long) i));

			assertEquals(i + 1, (long) fetchData.getCommitId());
			assertEquals(i + 1, fetchData.getLevel());
			assertDiffs(diffs, fetchData.getDiffs());

			totalDiffs.addAll(diffs);
		}

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.fetch(0L));

		assertEquals(3, (long) fetchData.getCommitId());
		assertEquals(3, fetchData.getLevel());
		assertDiffs(totalDiffs, fetchData.getDiffs());
	}

	@Test
	public void chunkRemovalSameCommit() {
		List<LogDiff<CubeDiff>> diffs = Arrays.asList(
				LogDiff.forCurrentPosition(CubeDiff.of(mapOf(
						"test", AggregationDiff.of(setOf(chunk(10)))))),
				LogDiff.forCurrentPosition(CubeDiff.of(mapOf(
						"test", AggregationDiff.of(emptySet(), setOf(chunk(10))))))
		);
		DiffsWithLevel<LogDiff<CubeDiff>> protoCommit = await(uplink.createProtoCommit(0L, diffs, 0));
		await(uplink.push(protoCommit));

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.checkout());

		assertTrue(fetchData.getDiffs().isEmpty());
	}

	@Test
	public void chunkRemovalTwoCommits() {
		List<LogDiff<CubeDiff>> diffs = singletonList(
				LogDiff.forCurrentPosition(CubeDiff.of(mapOf(
						"test", AggregationDiff.of(setOf(chunk(10)))))));
		DiffsWithLevel<LogDiff<CubeDiff>> protoCommit = await(uplink.createProtoCommit(0L, diffs, 0));
		await(uplink.push(protoCommit));

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.checkout());

		StubState state = new StubState();
		fetchData.getDiffs().forEach(state::apply);

		assertTrue(state.positions.isEmpty());
		assertEquals(mapOf("test", setOf(chunk(10))), state.chunks);

		diffs = singletonList(
				LogDiff.forCurrentPosition(CubeDiff.of(mapOf(
						"test", AggregationDiff.of(emptySet(), setOf(chunk(10)))))));
		protoCommit = await(uplink.createProtoCommit(1L, diffs, 1));
		await(uplink.push(protoCommit));

		fetchData = await(uplink.fetch(1L));
		fetchData.getDiffs().forEach(state::apply);

		assertTrue(state.positions.isEmpty());
		assertTrue(state.chunks.isEmpty());

		FetchData<Long, LogDiff<CubeDiff>> checkoutData = await(uplink.checkout());

		assertEquals(2, (long) checkoutData.getCommitId());
		assertEquals(2, checkoutData.getLevel());
		assertTrue(checkoutData.getDiffs().isEmpty());
	}

	@Test
	public void pushAlreadyPushed() {
		List<LogDiff<CubeDiff>> diffs = randomDiffs();

		DiffsWithLevel<LogDiff<CubeDiff>> protoCommit = await(uplink.createProtoCommit(0L, diffs, 0));
		await(uplink.push(protoCommit));

		Throwable exception = awaitException(uplink.push(protoCommit));
		assertThat(exception, instanceOf(OTException.class));
		assertEquals("Revision mismatch", exception.getMessage());
	}

	private static void assertDiffs(List<LogDiff<CubeDiff>> expected, List<LogDiff<CubeDiff>> actual) {
		StubState left = new StubState();
		StubState right = new StubState();

		expected.forEach(left::apply);
		actual.forEach(right::apply);

		assertEquals(left.chunks, right.chunks);
		assertEquals(left.positions, right.positions);
	}

	private static AggregationChunk chunk(long id) {
		return AggregationChunk.create(id, MEASURES, MIN_KEY, MAX_KEY, COUNT);
	}

	private static LogPosition randomLogPosition() {
		byte[] bytes = new byte[RANDOM.nextInt(10) + 1];
		RANDOM.nextBytes(bytes);
		return LogPosition.create(new LogFile(new String(bytes, UTF_8), RANDOM.nextInt()), RANDOM.nextLong());
	}

	private static List<LogDiff<CubeDiff>> randomDiffs() {
		List<LogDiff<CubeDiff>> diffs = new ArrayList<>();

		for (int i = 0; i < RANDOM.nextInt(5); i++) {
			Map<String, LogPositionDiff> positions = new HashMap<>();
			for (int j = 0; j < RANDOM.nextInt(5); j++) {
				LogPosition from;
				if (RANDOM.nextFloat() < 0.1) {
					from = LogPosition.initial();
				} else {
					from = randomLogPosition();
				}
				positions.put(i + "-" + j, new LogPositionDiff(from, randomLogPosition()));
			}

			List<CubeDiff> cubeDiffs = new ArrayList<>();
			for (int j = 0; j < RANDOM.nextInt(5); j++) {
				Map<String, AggregationDiff> aggregationDiffs = new HashMap<>();
				for (int k = 0; k < RANDOM.nextInt(5); k++) {
					Set<AggregationChunk> added = new HashSet<>();
					for (int k1 = 0; k1 < RANDOM.nextInt(5); k1++) {
						added.add(chunk(RANDOM.nextLong()));
					}
					aggregationDiffs.put(i + "-" + j + "-" + k, AggregationDiff.of(added));

				}
				cubeDiffs.add(CubeDiff.of(aggregationDiffs));
			}

			diffs.add(LogDiff.of(positions, cubeDiffs));
		}

		return diffs;
	}

	private static final class StubState implements OTState<LogDiff<CubeDiff>> {
		Map<String, LogPosition> positions = new HashMap<>();
		Map<String, Set<AggregationChunk>> chunks = new HashMap<>();

		@Override
		public void init() {
			positions.clear();
			chunks.clear();
		}

		@Override
		public void apply(LogDiff<CubeDiff> op) {
			for (Map.Entry<String, LogPositionDiff> entry : op.getPositions().entrySet()) {
				positions.put(entry.getKey(), entry.getValue().to);
			}
			for (CubeDiff diff : op.getDiffs()) {
				for (Map.Entry<String, AggregationDiff> entry : diff.entrySet()) {
					Set<AggregationChunk> set = chunks.computeIfAbsent(entry.getKey(), $ -> new HashSet<>());
					AggregationDiff aggregationDiff = entry.getValue();
					set.addAll(aggregationDiff.getAddedChunks());
					set.removeAll(aggregationDiff.getRemovedChunks());
					if (set.isEmpty()) {
						chunks.remove(entry.getKey());
					}
				}
			}
		}
	}
}
