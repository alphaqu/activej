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

package io.activej.aggregation.util;

import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.DecoderFunction;

import java.util.function.Function;

public interface StringCodec<T> {
	String toString(T value);

	T fromString(String string) throws MalformedDataException;

	static <T> StringCodec<T> of(DecoderFunction<String, T> decoder, Function<T, String> encoder) {
		return new StringCodec<T>() {
			@Override
			public String toString(T value) {
				return encoder.apply(value);
			}

			@Override
			public T fromString(String string) throws MalformedDataException {
				try {
					return decoder.decode(string);
				} catch (MalformedDataException e) {
					throw e;
				} catch (Exception e) {
					throw new MalformedDataException(e);
				}
			}
		};
	}
}
