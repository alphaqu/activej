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

import io.activej.common.exception.MalformedDataException;
import io.activej.cube.Cube;

import java.util.List;
import java.util.stream.Collectors;

public interface MeasuresValidator {

	void validate(String aggregationId, List<String> measures) throws MalformedDataException;

	static MeasuresValidator ofCube(Cube cube) {
		return (aggregationId, measures) -> {
			List<String> aggregationMeasures = cube.getAggregation(aggregationId).getMeasures();
			List<String> unknownMeasures = measures.stream()
					.filter(measure -> !aggregationMeasures.contains(measure))
					.collect(Collectors.toList());
			if (!unknownMeasures.isEmpty()) {
				throw new MalformedDataException(String.format("Unknown measures %s in aggregation '%s'", unknownMeasures, aggregationId));
			}
		};
	}
}