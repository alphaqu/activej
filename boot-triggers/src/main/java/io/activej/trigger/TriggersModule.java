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

package io.activej.trigger;

import io.activej.common.initializer.Initializer;
import io.activej.common.initializer.WithInitializer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Optional;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.AbstractModule;
import io.activej.launcher.LauncherService;
import io.activej.trigger.util.KeyWithWorkerData;
import io.activej.trigger.util.Utils;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPools;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.trigger.util.Utils.prettyPrintSimpleKeyName;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;

@SuppressWarnings("unused")
public final class TriggersModule extends AbstractModule implements TriggersModuleSettings, WithInitializer<TriggersModule> {
	private Function<Key<?>, String> keyToString = Utils::prettyPrintSimpleKeyName;

	private final Map<Class<?>, Set<TriggerConfig<?>>> classSettings = new LinkedHashMap<>();
	private final Map<Key<?>, Set<TriggerConfig<?>>> keySettings = new LinkedHashMap<>();

	private static final class TriggerConfig<T> {
		private final Severity severity;
		private final String name;
		private final Function<T, TriggerResult> triggerFunction;

		TriggerConfig(Severity severity, String name,
				Function<T, TriggerResult> triggerFunction) {
			this.severity = severity;
			this.name = name;
			this.triggerFunction = triggerFunction;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TriggerConfig<?> that = (TriggerConfig<?>) o;
			return severity == that.severity &&
					Objects.equals(name, that.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(severity, name);
		}
	}

	private static final class TriggerRegistryRecord {
		private final Severity severity;
		private final String name;
		private final Supplier<TriggerResult> triggerFunction;

		private TriggerRegistryRecord(Severity severity, String name, Supplier<TriggerResult> triggerFunction) {
			this.severity = severity;
			this.name = name;
			this.triggerFunction = triggerFunction;
		}
	}

	private TriggersModule() {
	}

	public static TriggersModule create() {
		return new TriggersModule();
	}

	@Override
	public TriggersModule withNaming(Function<Key<?>, String> keyToString) {
		this.keyToString = keyToString;
		return this;
	}

	@Override
	public <T> TriggersModule with(Class<T> type, Severity severity, String name, Function<T, TriggerResult> triggerFunction) {
		Set<TriggerConfig<?>> triggerConfigs = classSettings.computeIfAbsent(type, $ -> new LinkedHashSet<>());

		if (!triggerConfigs.add(new TriggerConfig<>(severity, name, triggerFunction))) {
			throw new IllegalArgumentException("Cannot assign duplicate trigger");
		}

		return this;
	}

	@Override
	public <T> TriggersModule with(Key<T> key, Severity severity, String name, Function<T, TriggerResult> triggerFunction) {
		Set<TriggerConfig<?>> triggerConfigs = keySettings.computeIfAbsent(key, $ -> new LinkedHashSet<>());

		if (!triggerConfigs.add(new TriggerConfig<>(severity, name, triggerFunction))) {
			throw new IllegalArgumentException("Cannot assign duplicate trigger");
		}

		return this;
	}

	@Provides
	Triggers triggers() {
		return Triggers.create();
	}

	@ProvidesIntoSet
	LauncherService service(Injector injector, Triggers triggers, @Optional Set<Initializer<TriggersModuleSettings>> initializers) {
		if (initializers != null) {
			for (Initializer<TriggersModuleSettings> initializer : initializers) {
				initializer.accept(this);
			}
		}
		return new LauncherService() {
			@Override
			public CompletableFuture<?> start() {
				doStart(injector, triggers);
				return completedFuture(null);
			}

			@Override
			public CompletableFuture<?> stop() {
				return completedFuture(null);
			}
		};
	}

	@SuppressWarnings("unchecked")
	private void doStart(Injector injector, Triggers triggers) {
		Map<KeyWithWorkerData, List<TriggerRegistryRecord>> triggersMap = new LinkedHashMap<>();

		// register singletons
		for (Map.Entry<Key<?>, Object> entry : injector.peekInstances().entrySet()) {
			Key<Object> key = (Key<Object>) entry.getKey();
			Object instance = entry.getValue();
			if (instance == null) continue;
			KeyWithWorkerData internalKey = new KeyWithWorkerData(key);

			scanHasTriggers(triggersMap, internalKey, instance);
			scanClassSettings(triggersMap, internalKey, instance);
			scanKeySettings(triggersMap, internalKey, instance);
		}

		// register workers
		WorkerPools workerPools = injector.peekInstance(WorkerPools.class);
		if (workerPools != null) {
			for (WorkerPool workerPool : workerPools.getWorkerPools()) {
				for (Map.Entry<Key<?>, WorkerPool.Instances<?>> entry : workerPool.peekInstances().entrySet()) {
					Key<?> key = entry.getKey();
					WorkerPool.Instances<?> workerInstances = entry.getValue();
					for (int i = 0; i < workerInstances.size(); i++) {
						Object workerInstance = workerInstances.get(i);
						KeyWithWorkerData k = new KeyWithWorkerData(key, workerPool, i);

						scanHasTriggers(triggersMap, k, workerInstance);
						scanClassSettings(triggersMap, k, workerInstance);
						scanKeySettings(triggersMap, k, workerInstance);
					}
				}

			}
		}

		for (KeyWithWorkerData keyWithWorkerData : triggersMap.keySet()) {
			for (TriggerRegistryRecord registryRecord : triggersMap.getOrDefault(keyWithWorkerData, emptyList())) {
				triggers.addTrigger(registryRecord.severity, prettyPrintSimpleKeyName(keyWithWorkerData.getKey()), registryRecord.name, registryRecord.triggerFunction);
			}
		}
	}

	private void scanHasTriggers(Map<KeyWithWorkerData, List<TriggerRegistryRecord>> triggers, KeyWithWorkerData internalKey, Object instance) {
		if (instance instanceof HasTriggers) {
			((HasTriggers) instance).registerTriggers(new TriggerRegistry() {
				@Override
				public Key<?> getComponentKey() {
					return internalKey.getKey();
				}

				@Override
				public String getComponentName() {
					return keyToString.apply(internalKey.getKey());
				}

				@Override
				public void add(Severity severity, String name, Supplier<TriggerResult> triggerFunction) {
					triggers.computeIfAbsent(internalKey, $ -> new ArrayList<>()).add(new TriggerRegistryRecord(severity, name, triggerFunction));
				}
			});
		}
	}

	@SuppressWarnings({"unchecked", "RedundantCast"})
	private void scanClassSettings(Map<KeyWithWorkerData, List<TriggerRegistryRecord>> triggers, KeyWithWorkerData internalKey, Object instance) {
		for (Map.Entry<Class<?>, Set<TriggerConfig<?>>> entry : classSettings.entrySet()) {
			for (TriggerConfig<?> config : entry.getValue()) {
				if (entry.getKey().isAssignableFrom(instance.getClass())) {
					triggers.computeIfAbsent(internalKey, $ -> new ArrayList<>())
							.add(new TriggerRegistryRecord(config.severity, config.name, () ->
									((TriggerConfig<Object>) config).triggerFunction.apply(instance)));
				}
			}
		}
	}

	@SuppressWarnings({"unchecked", "RedundantCast"})
	private void scanKeySettings(Map<KeyWithWorkerData, List<TriggerRegistryRecord>> triggers, KeyWithWorkerData internalKey, Object instance) {
		Key<Object> key = (Key<Object>) internalKey.getKey();
		for (TriggerConfig<?> config : keySettings.getOrDefault(key, emptySet())) {
			triggers.computeIfAbsent(internalKey, $ -> new ArrayList<>())
					.add(new TriggerRegistryRecord(config.severity, config.name, () ->
							((TriggerConfig<Object>) config).triggerFunction.apply(instance)));
		}
	}

}
