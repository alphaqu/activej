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

package io.activej.aggregation;

import io.activej.aggregation.util.StringCodec;
import io.activej.common.exception.MalformedDataException;

public interface ChunkIdCodec<C> extends StringCodec<C> {
	String toFileName(C chunkId);

	C fromFileName(String chunkFileName) throws MalformedDataException;

	static ChunkIdCodec<Long> ofLong() {
		return new ChunkIdCodec<Long>() {
			@Override
			public String toFileName(Long chunkId) {
				return chunkId.toString();
			}

			@Override
			public Long fromFileName(String chunkFileName) throws MalformedDataException {
				try {
					return Long.parseLong(chunkFileName);
				} catch (NumberFormatException e) {
					throw new MalformedDataException(e);
				}
			}

			@Override
			public String toString(Long value) {
				return value.toString();
			}

			@Override
			public Long fromString(String string) throws MalformedDataException {
				try {
					return Long.parseLong(string);
				} catch (NumberFormatException e) {
					throw new MalformedDataException(e);
				}
			}
		};
	}

	static ChunkIdCodec<String> ofString() {
		return new ChunkIdCodec<String>() {
			@Override
			public String toFileName(String chunkId) {
				return chunkId;
			}

			@Override
			public String fromFileName(String chunkFileName) {
				return chunkFileName;
			}

			@Override
			public String toString(String value) {
				return value;
			}

			@Override
			public String fromString(String string) {
				return string;
			}
		};
	}
}
