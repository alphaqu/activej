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

package io.activej.rpc.protocol;

import io.activej.common.MemSize;
import io.activej.async.exception.AsyncCloseException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.datastream.AbstractStreamConsumer;
import io.activej.datastream.AbstractStreamSupplier;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

public final class RpcStream {
	private final ChannelDeserializer<RpcMessage> deserializer;
	private final ChannelSerializer<RpcMessage> serializer;
	private Listener listener;

	private final AbstractStreamConsumer<RpcMessage> internalConsumer = new AbstractStreamConsumer<RpcMessage>() {};

	private final AbstractStreamSupplier<RpcMessage> internalSupplier = new AbstractStreamSupplier<RpcMessage>() {
		@Override
		protected void onResumed() {
			deserializer.updateDataAcceptor();
			//noinspection ConstantConditions - dataAcceptorr is not null in onResumed state
			listener.onSenderReady(getDataAcceptor());
		}

		@Override
		protected void onSuspended() {
			if (server) {
				deserializer.updateDataAcceptor();
			}
			listener.onSenderSuspended();
		}

	};

	public interface Listener extends StreamDataAcceptor<RpcMessage> {
		void onReceiverEndOfStream();

		void onReceiverError(@NotNull Throwable e);

		void onSenderError(@NotNull Throwable e);

		void onSerializationError(RpcMessage message, @NotNull Throwable e);

		void onSenderReady(@NotNull StreamDataAcceptor<RpcMessage> acceptor);

		void onSenderSuspended();
	}

	private final boolean server;
	private final AsyncTcpSocket socket;

	public RpcStream(AsyncTcpSocket socket,
			BinarySerializer<RpcMessage> messageSerializer,
			MemSize initialBufferSize,
			Duration autoFlushInterval, @Nullable FrameFormat frameFormat, boolean server) {
		this.server = server;
		this.socket = socket;

		ChannelSerializer<RpcMessage> serializer = ChannelSerializer.create(messageSerializer)
				.withInitialBufferSize(initialBufferSize)
				.withAutoFlushInterval(autoFlushInterval)
				.withSerializationErrorHandler((message, e) -> listener.onSerializationError(message, e));
		ChannelDeserializer<RpcMessage> deserializer = ChannelDeserializer.create(messageSerializer);

		if (frameFormat != null) {
			ChannelFrameDecoder decompressor = ChannelFrameDecoder.create(frameFormat);
			ChannelFrameEncoder compressor = ChannelFrameEncoder.create(frameFormat);

			ChannelSupplier.ofSocket(socket).bindTo(decompressor.getInput());
			decompressor.getOutput().bindTo(deserializer.getInput());

			serializer.getOutput().bindTo(compressor.getInput());
			compressor.getOutput().set(ChannelConsumer.ofSocket(socket));
		} else {
			ChannelSupplier.ofSocket(socket).bindTo(deserializer.getInput());
			serializer.getOutput().set(ChannelConsumer.ofSocket(socket));
		}

		deserializer.streamTo(internalConsumer);

		this.deserializer = deserializer;
		this.serializer = serializer;
	}

	public void setListener(Listener listener) {
		this.listener = listener;
		deserializer.getEndOfStream()
				.whenResult(listener::onReceiverEndOfStream)
				.whenException(listener::onReceiverError);
		serializer.getAcknowledgement()
				.whenException(listener::onSenderError);
		internalSupplier.streamTo(serializer);
		internalConsumer.resume(this.listener);
	}

	public void receiverSuspend() {
		internalConsumer.suspend();
	}

	public void receiverResume() {
		internalConsumer.resume(listener);
	}

	public void sendEndOfStream() {
		internalSupplier.sendEndOfStream();
	}

	public void close() {
		closeEx(new AsyncCloseException("RPC Channel Closed"));
	}

	public void closeEx(@NotNull Throwable e) {
		socket.closeEx(e);
		serializer.closeEx(e);
		deserializer.closeEx(e);
	}
}
