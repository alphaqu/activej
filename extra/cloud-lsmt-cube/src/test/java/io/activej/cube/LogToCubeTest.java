package io.activej.cube;

import io.activej.aggregation.ActiveFsChunkStorage;
import io.activej.aggregation.AggregationChunkStorage;
import io.activej.aggregation.ChunkIdCodec;
import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.cube.bean.TestPubRequest;
import io.activej.cube.bean.TestPubRequest.TestAdvRequest;
import io.activej.cube.linear.CubeUplinkMySql;
import io.activej.cube.linear.PrimaryKeyCodecs;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeOT;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOT;
import io.activej.etl.LogOTProcessor;
import io.activej.etl.LogOTState;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.multilog.Multilog;
import io.activej.multilog.MultilogImpl;
import io.activej.ot.OTStateManager;
import io.activej.ot.system.OTSystem;
import io.activej.serializer.SerializerBuilder;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.aggregation.AggregationPredicates.alwaysTrue;
import static io.activej.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.cube.TestUtils.initializeUplink;
import static io.activej.cube.TestUtils.runProcessLogs;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.dataSource;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public final class LogToCubeTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Test
	public void testStubStorage() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Executor executor = Executors.newCachedThreadPool();
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		LocalActiveFs fs = LocalActiveFs.create(eventloop, executor, aggregationsDir);
		await(fs.start());
		FrameFormat frameFormat = LZ4FrameFormat.create();
		AggregationChunkStorage<Long> aggregationChunkStorage = ActiveFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), frameFormat, fs);
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("pub", ofInt())
				.withDimension("adv", ofInt())
				.withMeasure("pubRequests", sum(ofLong()))
				.withMeasure("advRequests", sum(ofLong()))
				.withAggregation(id("pub").withDimensions("pub").withMeasures("pubRequests"))
				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests"));

		DataSource dataSource = dataSource("test.properties");
		CubeUplinkMySql uplink = CubeUplinkMySql.create(executor, dataSource, PrimaryKeyCodecs.ofCube(cube));
		initializeUplink(uplink);

		List<TestAdvResult> expected = asList(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));

		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube);
		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(eventloop, otSystem, uplink, cubeDiffLogOTState);

		LocalActiveFs localFs = LocalActiveFs.create(eventloop, executor, logsDir);
		await(localFs.start());
		Multilog<TestPubRequest> multilog = MultilogImpl.create(eventloop, localFs,
				frameFormat,
				SerializerBuilder.create(classLoader).build(TestPubRequest.class),
				NAME_PARTITION_REMAINDER_SEQ);

		LogOTProcessor<TestPubRequest, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop,
				multilog,
				new TestAggregatorSplitter(cube), // TestAggregatorSplitter.create(eventloop, cube),
				"testlog",
				asList("partitionA"),
				cubeDiffLogOTState);

		StreamSupplier<TestPubRequest> supplier = StreamSupplier.of(
				new TestPubRequest(1000, 1, asList(new TestAdvRequest(10))),
				new TestPubRequest(1001, 2, asList(new TestAdvRequest(10), new TestAdvRequest(20))),
				new TestPubRequest(1002, 1, asList(new TestAdvRequest(30))),
				new TestPubRequest(1002, 2, Arrays.asList()));

		await(supplier.streamTo(StreamConsumer.ofPromise(multilog.write("partitionA"))));
		await(logCubeStateManager.checkout());
		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		List<TestAdvResult> list = await(cube.queryRawStream(
				asList("adv"),
				asList("advRequests"),
				alwaysTrue(),
				TestAdvResult.class, classLoader)
				.toList());

		assertEquals(expected, list);
	}

	public static final class TestAdvResult {
		public int adv;
		public long advRequests;

		@SuppressWarnings("unused")
		public TestAdvResult() {
		}

		public TestAdvResult(int adv, long advRequests) {
			this.adv = adv;
			this.advRequests = advRequests;
		}

		@Override
		public String toString() {
			return "TestAdvResult{adv=" + adv + ", advRequests=" + advRequests + '}';
		}

		@SuppressWarnings("RedundantIfStatement")
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestAdvResult that = (TestAdvResult) o;

			if (adv != that.adv) return false;
			if (advRequests != that.advRequests) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = adv;
			result = 31 * result + (int) (advRequests ^ (advRequests >>> 32));
			return result;
		}
	}
}
