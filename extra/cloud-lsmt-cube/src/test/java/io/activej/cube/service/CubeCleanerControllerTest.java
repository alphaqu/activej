package io.activej.cube.service;

import io.activej.aggregation.ActiveFsChunkStorage;
import io.activej.aggregation.AggregationChunkStorage;
import io.activej.aggregation.ChunkIdCodec;
import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.cube.Cube;
import io.activej.cube.IdGeneratorStub;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeUplinkMySql;
import io.activej.cube.ot.CubeUplinkMySql.UplinkProtoCommit;
import io.activej.cube.ot.PrimaryKeyCodecs;
import io.activej.etl.LogDiff;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.dataSource;
import static java.util.Collections.emptyList;

public class CubeCleanerControllerTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private Eventloop eventloop;
	private CubeUplinkMySql uplink;
	private AggregationChunkStorage<Long> aggregationChunkStorage;

	@Before
	public void setUp() throws Exception {
		DataSource dataSource = dataSource("test.properties");
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Executor executor = Executors.newCachedThreadPool();

		eventloop = Eventloop.getCurrentEventloop();

		DefiningClassLoader classLoader = DefiningClassLoader.create();
		aggregationChunkStorage = ActiveFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(),
				LZ4FrameFormat.create(), LocalActiveFs.create(eventloop, executor, aggregationsDir));
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("pub", ofInt())
				.withDimension("adv", ofInt())
				.withMeasure("pubRequests", sum(ofLong()))
				.withMeasure("advRequests", sum(ofLong()))
				.withAggregation(id("pub").withDimensions("pub").withMeasures("pubRequests"))
				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests"));

		uplink = CubeUplinkMySql.create(executor, dataSource, PrimaryKeyCodecs.ofCube(cube));
		uplink.initialize();
		uplink.truncateTables();
	}

	@Test
	public void testCleanupWithExtraSnapshotsCount() throws IOException, SQLException {
		// 1S -> 2N -> 3N -> 4S -> 5N
		initializeRepo();

		CubeCleanerController<Long, LogDiff<CubeDiff>, Long> cleanerController = CubeCleanerController.create(eventloop,
						uplink, (ActiveFsChunkStorage<Long>) aggregationChunkStorage)
				.withChunksCleanupDelay(Duration.ofMillis(0));

		await(cleanerController.cleanup());
	}

	@Test
	public void testCleanupWithFreezeTimeout() throws IOException, SQLException {
		// 1S -> 2N -> 3N -> 4S -> 5N
		initializeRepo();

		CubeCleanerController<Long, LogDiff<CubeDiff>, Long> cleanerController = CubeCleanerController.create(eventloop,
						uplink, (ActiveFsChunkStorage<Long>) aggregationChunkStorage)
				.withChunksCleanupDelay(Duration.ofSeconds(10));

		await(cleanerController.cleanup());
	}

	public void initializeRepo() throws IOException, SQLException {
		uplink.initialize();
		uplink.truncateTables();

		UplinkProtoCommit proto1 = await(uplink.createProtoCommit(0L, emptyList(), 0));
		await(uplink.push(proto1)); // 1N

		UplinkProtoCommit proto2 = await(uplink.createProtoCommit(1L, emptyList(), 1));
		await(uplink.push(proto2)); // 2N

		UplinkProtoCommit proto3 = await(uplink.createProtoCommit(2L, emptyList(), 2));
		await(uplink.push(proto3)); // 3N

		UplinkProtoCommit proto4 = await(uplink.createProtoCommit(3L, emptyList(), 3));
		await(uplink.push(proto4)); // 4S

		UplinkProtoCommit proto5 = await(uplink.createProtoCommit(4L, emptyList(), 4));
		await(uplink.push(proto5)); // 5N
	}
}
