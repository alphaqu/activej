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

package io.activej.aggregation.fieldtype;

import io.activej.aggregation.util.StringCodec;
import io.activej.common.exception.MalformedDataException;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import static java.util.stream.Collectors.joining;

class StringCodecs {
	static final StringCodec<String> STRING_CODEC = StringCodec.of($ -> $, $ -> $);
	static final StringCodec<Short> SHORT_CODEC = StringCodec.of(Short::parseShort, Object::toString);
	static final StringCodec<Integer> INTEGER_CODEC = StringCodec.of(Integer::parseInt, Object::toString);
	static final StringCodec<Long> LONG_CODEC = StringCodec.of(Long::parseLong, Object::toString);
	static final StringCodec<Float> FLOAT_CODEC = StringCodec.of(Float::parseFloat, Object::toString);
	static final StringCodec<Double> DOUBLE_CODEC = StringCodec.of(Double::parseDouble, Object::toString);
	static final StringCodec<Boolean> BOOLEAN_CODEC = StringCodec.of(Boolean::parseBoolean, Object::toString);
	static final StringCodec<Byte> BYTE_CODEC = StringCodec.of(Byte::parseByte, Object::toString);
	static final StringCodec<Character> CHARACTER_CODEC = StringCodec.of(string -> {
		char[] chars = string.toCharArray();
		if (chars.length != 1) {
			throw new MalformedDataException("Malformed character string");
		}
		return chars[0];
	}, Object::toString);

	static final StringCodec<LocalDate> LOCAL_DATE_CODEC = StringCodec.of(
			LocalDate::parse, LocalDate::toString
	);

	static <T> StringCodec<Set<T>> ofSet(StringCodec<T> codec) {
		return new StringCodec<Set<T>>() {

			@Override
			public String toString(Set<T> value) {
				return value.stream()
						.map(codec::toString)
						.collect(joining(" "));
			}

			@Override
			public Set<T> fromString(String string) throws MalformedDataException {
				Set<T> set = new HashSet<>();
				for (String s : string.split(" ")) {
					set.add(codec.fromString(s));
				}
				return set;
			}
		};
	}

	static <E extends Enum<E>> StringCodec<E> ofEnum(Class<E> enumClass) {
		return StringCodec.of(string -> Enum.valueOf(enumClass, string), Enum::name);
	}
}
