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

package io.activej.common.collection;

import io.activej.common.recycle.Recyclers;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.*;

public final class Either<L, R> {
	static {
		Recyclers.register(Either.class, either -> {
			Recyclers.recycle(either.left);
			Recyclers.recycle(either.right);
		});
	}

	@Nullable
	private final L left;

	@Nullable
	private final R right;

	private final boolean isRight; // so that this either supports nulls

	private Either(@Nullable L left, @Nullable R right, boolean isRight) {
		this.left = left;
		this.right = right;
		this.isRight = isRight;
	}

	public static <L, R> Either<L, R> left(@Nullable L left) {
		return new Either<>(left, null, false);
	}

	public static <L, R> Either<L, R> right(@Nullable R right) {
		return new Either<>(null, right, true);
	}

	@Contract(pure = true)
	public boolean isLeft() {
		return !isRight;
	}

	@Contract(pure = true)
	public boolean isRight() {
		return isRight;
	}

	@Contract(pure = true)
	@Nullable
	public L getLeft() {
		return left;
	}

	@Contract(pure = true)
	@Nullable
	public R getRight() {
		return right;
	}

	@Contract(pure = true)
	public L getLeftElse(@Nullable L defaultValue) {
		return isLeft() ? left : defaultValue;
	}

	@Contract(pure = true)
	public R getRightElse(@Nullable R defaultValue) {
		return isRight() ? right : defaultValue;
	}

	@Contract(pure = true)
	public L getLeftElseGet(@NotNull Supplier<? extends L> defaultValueSupplier) {
		return isLeft() ? left : defaultValueSupplier.get();
	}

	@Contract(pure = true)
	public R getRightElseGet(@NotNull Supplier<? extends R> defaultValueSupplier) {
		return isRight() ? right : defaultValueSupplier.get();
	}

	@Contract(pure = true)
	@NotNull
	public Either<L, R> ifLeft(@NotNull Consumer<? super L> leftConsumer) {
		if (isLeft()) {
			leftConsumer.accept(left);
		}
		return this;
	}

	@Contract(pure = true)
	@NotNull
	public Either<L, R> ifRight(@NotNull Consumer<? super R> rightConsumer) {
		if (isRight()) {
			rightConsumer.accept(right);
		}
		return this;
	}

	@Contract(pure = true)
	@NotNull
	public Either<L, R> consume(@NotNull BiConsumer<? super L, ? super R> consumer) {
		consumer.accept(left, right);
		return this;
	}

	@Contract(pure = true)
	@NotNull
	public Either<L, R> consume(@NotNull Consumer<? super L> leftConsumer, @NotNull Consumer<? super R> rightConsumer) {
		if (isLeft()) {
			leftConsumer.accept(left);
		} else {
			rightConsumer.accept(right);
		}
		return this;
	}

	@Contract(pure = true)
	public <U> U reduce(@NotNull Function<? super L, ? extends U> leftFn, @NotNull Function<? super R, ? extends U> rightFn) {
		return isLeft() ? leftFn.apply(left) : rightFn.apply(right);
	}

	@Contract(pure = true)
	public <U> U reduce(@NotNull BiFunction<? super L, ? super R, ? extends U> fn) {
		return fn.apply(left, right);
	}

	@Contract(pure = true)
	@NotNull
	public Either<R, L> swap() {
		return new Either<>(right, left, !isRight);
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public <T> Either<T, R> mapLeft(@NotNull Function<? super L, ? extends T> fn) {
		return isLeft() ?
				new Either<>(fn.apply(left), null, false) :
				(Either<T, R>) this;
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public <T> Either<L, T> mapRight(@NotNull Function<? super R, ? extends T> fn) {
		return isRight() ?
				new Either<>(null, fn.apply(right), true) :
				(Either<L, T>) this;
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public <T> Either<T, R> flatMapLeft(@NotNull Function<? super L, Either<T, R>> fn) {
		return isLeft() ?
				fn.apply(left) :
				(Either<T, R>) this;
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public <T> Either<L, T> flatMapRight(@NotNull Function<? super R, Either<L, T>> fn) {
		return isRight() ?
				fn.apply(right) :
				(Either<L, T>) this;
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Either<?, ?> either = (Either<?, ?>) o;
		if (left != null ? !left.equals(either.left) : either.left != null) return false;
		if (right != null ? !right.equals(either.right) : either.right != null) return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = 0;
		result = 31 * result + (left != null ? left.hashCode() : 0);
		result = 31 * result + (right != null ? right.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "{" + (isLeft() ? "left=" + left : "left=" + left) + "}";
	}
}
