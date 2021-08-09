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

import io.activej.aggregation.Aggregation;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.aggregation.util.StringCodec;
import io.activej.common.exception.MalformedDataException;
import io.activej.cube.Cube;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public final class PrimaryKeyCodecs {
	private final Function<String, @Nullable StringCodec<PrimaryKey>> lookUpFn;

	private PrimaryKeyCodecs(Function<String, @Nullable StringCodec<PrimaryKey>> lookUpFn) {
		this.lookUpFn = lookUpFn;
	}

	@SuppressWarnings("unchecked")
	public static PrimaryKeyCodecs ofCube(Cube cube) {
		Map<String, StringCodec<PrimaryKey>> codecMap = new HashMap<>();
		for (String aggregationId : cube.getAggregationIds()) {
			Aggregation aggregation = cube.getAggregation(aggregationId);
			AggregationStructure structure = aggregation.getStructure();
			int size = structure.getKeys().size();
			StringCodec<Object>[] codecs = new StringCodec[size];
			for (int i = 0; i < size; i++) {
				String key = structure.getKeys().get(i);
				FieldType<?> keyType = structure.getKeyTypes().get(key);
				codecs[i] = (StringCodec<Object>) keyType.getInternalCodec();
			}
			StringCodec<PrimaryKey> codec = new StringCodec<PrimaryKey>() {
				@Override
				public String toString(PrimaryKey primaryKey) {
					StringBuilder sb = new StringBuilder();
					Object[] array = primaryKey.getArray();
					if (array.length != size) throw new IllegalArgumentException("Primary key length mismatch");

					for (int i = 0; i < size; i++) {
						sb.append(codecs[i].toString(array[i]));
						if (i != array.length - 1) {
							sb.append(" ");
						}
					}
					return sb.toString();
				}

				@Override
				public PrimaryKey fromString(String primaryKeyString) throws MalformedDataException {
					String[] parts = primaryKeyString.split(" ");
					if (parts.length != size) throw new MalformedDataException("Primary key length mismatch");

					Object[] values = new Object[size];
					for (int i = 0; i < size; i++) {
						try {
							values[i] = codecs[i].fromString(parts[i]);
						} catch (Exception e) {
							throw new MalformedDataException(e);
						}
					}

					return PrimaryKey.ofArray(values);
				}
			};
			codecMap.put(aggregationId, codec);
		}
		return new PrimaryKeyCodecs(codecMap::get);
	}

	public static PrimaryKeyCodecs ofLookUp(Function<String, @Nullable StringCodec<PrimaryKey>> lookUpFn) {
		return new PrimaryKeyCodecs(lookUpFn);
	}

	public String toString(String aggregationId, PrimaryKey value) {
		StringCodec<PrimaryKey> codec = lookUpFn.apply(aggregationId);
		if (codec == null) throw new IllegalArgumentException("Unknown aggregation '" + aggregationId + '\'');
		return codec.toString(value);
	}

	public PrimaryKey fromString(String aggregationId, String string) throws MalformedDataException {
		StringCodec<PrimaryKey> codec = lookUpFn.apply(aggregationId);
		if (codec == null) throw new MalformedDataException("Unknown aggregation '" + aggregationId + '\'');
		return codec.fromString(string);
	}
}
