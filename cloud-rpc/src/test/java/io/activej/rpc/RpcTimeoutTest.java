package io.activej.rpc;

import io.activej.async.exception.AsyncTimeoutException;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.server.RpcServer;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

public final class RpcTimeoutTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();
	public static final String DATA = "Test";

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private static final int SERVER_DELAY = 100;

	private RpcClient client;
	private RpcServer server;

	@Before
	public void setUp() throws Exception {
		int port = getFreePort();
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Executor executor = Executors.newSingleThreadExecutor();
		List<Class<?>> messageTypes = singletonList(String.class);

		server = RpcServer.create(eventloop)
				.withMessageTypes(messageTypes)
				.withHandler(String.class,
						request -> Promise.ofBlockingCallable(executor, () -> {
							Thread.sleep(SERVER_DELAY);
							return request;
						}))
				.withListenPort(port);

		client = RpcClient.create(eventloop)
				.withMessageTypes(messageTypes)
				.withStrategy(server(new InetSocketAddress(port)));

		server.listen();
	}

	@Test
	public void noTimeout() {
		String res = (String) await(client.start()
				.then(() -> client.sendRequest(DATA))
				.then(response -> client.stop()
						.then(server::close)
						.map($ -> response)));

		assertEquals(DATA, res);
	}

	@Test
	public void shouldNotTimeout() {
		int timeout = SERVER_DELAY * 2;
		String res = (String) await(client.start()
				.then(() -> client.sendRequest(DATA, timeout))
				.then(response -> client.stop()
						.then(server::close)
						.map($ -> response)));

		assertEquals(DATA, res);
	}

	@Test
	public void shouldTimeout() {
		int timeout = SERVER_DELAY / 2;
		//noinspection ConstantConditions
		Throwable exception = awaitException(client.start()
				.then(() -> client.sendRequest(DATA, timeout))
				.thenEx(($, e) -> client.stop()
						.then(server::close)
						.then($2 -> Promise.ofException(e))));

		assertThat(exception, instanceOf(AsyncTimeoutException.class));
	}
}
