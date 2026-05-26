/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

/// This class corresponds to Flink JoinedRowData
/// (org.apache.flink.table.data.utils.JoinedRowData).
///
/// JoinedRowData in Flink represents a single logical row formed by
/// concatenating the fields of two backing RowData. In Velox, a RowVector
/// represents a *batch* of rows, so this class concatenates the *columns*
/// (children) of two backing RowVectors that share the same number of rows;
/// each row in the resulting JoinedRowVector therefore corresponds to the
/// concatenation of one row from row1 and one row from row2.
///
/// The class is mutable to allow performant in-place updates (mirroring the
/// rationale documented on Flink's JoinedRowData):
///   - The backing rows can be swapped via replace(row1, row2) without
///     allocating a new wrapper.
///   - The combined RowType is lazily (re)computed when needed.
///   - The RowKind can be mutated independently via setRowKind().
///
/// Notes on semantics:
///   - childAt(pos) returns the underlying child VectorPtr - prefer it over
///     per-field accessors. Use the standard Velox APIs on the returned
///     vector (e.g. asFlatVector<T>()->valueAt(rowIdx), isNullAt(rowIdx)).
///   - The RowKind applies to the whole logical row (all rows in the batch),
///     matching Flink's per-row RowData semantics. If a batch needs to carry
///     mixed kinds, use a dedicated changelog column instead.
class JoinedRowVector {
 public:
  /// Creates an empty JoinedRowVector of kind INSERT without backing rows.
  /// The backing rows must be set via replace() before any access methods
  /// are called.
  JoinedRowVector() = default;

  /// Creates a JoinedRowVector of kind INSERT backed by 'row1' and 'row2'.
  /// Both rows must be non-null and must have the same number of rows.
  JoinedRowVector(RowVectorPtr row1, RowVectorPtr row2)
      : JoinedRowVector(RowKind::INSERT, std::move(row1), std::move(row2)) {}

  /// Creates a JoinedRowVector with explicit 'rowKind' backed by 'row1' and
  /// 'row2'. Both rows must be non-null and must have the same number of
  /// rows.
  JoinedRowVector(RowKind rowKind, RowVectorPtr row1, RowVectorPtr row2)
      : rowKind_(rowKind) {
    replace(std::move(row1), std::move(row2));
  }

  /// Replaces the backing RowVectors in place. Returns *this for chaining,
  /// mirroring Flink JoinedRowData::replace. The current RowKind is
  /// preserved; use setRowKind() to change it.
  ///
  /// Replacement does NOT copy any underlying data - the children of the two
  /// input vectors are referenced as-is. Both rows must be non-null and have
  /// the same row count.
  JoinedRowVector& replace(RowVectorPtr row1, RowVectorPtr row2) {
    VELOX_CHECK_NOT_NULL(row1, "JoinedRowVector requires non-null row1");
    VELOX_CHECK_NOT_NULL(row2, "JoinedRowVector requires non-null row2");
    VELOX_CHECK_EQ(
        row1->size(),
        row2->size(),
        "JoinedRowVector requires both backing rows to have the same row count");
    row1_ = std::move(row1);
    row2_ = std::move(row2);
    // Invalidate the cached combined type; it is rebuilt on demand.
    combinedType_.reset();
    return *this;
  }

  RowKind rowKind() const {
    return rowKind_;
  }

  void setRowKind(RowKind kind) {
    rowKind_ = kind;
  }

  const RowVectorPtr& row1() const {
    return row1_;
  }

  const RowVectorPtr& row2() const {
    return row2_;
  }

  /// Returns the number of fields (columns), equivalent to Flink's
  /// RowData::getArity().
  size_t arity() const {
    ensureBacked();
    return row1_->childrenSize() + row2_->childrenSize();
  }

  size_t childrenSize() const {
    return arity();
  }

  /// Returns the number of rows in the underlying batch.
  vector_size_t size() const {
    ensureBacked();
    return row1_->size();
  }

  /// Returns the child VectorPtr at position 'pos'. Positions
  /// [0, row1.arity) map to row1; positions
  /// [row1.arity, row1.arity + row2.arity) map to row2.
  const VectorPtr& childAt(size_t pos) const {
    ensureBacked();
    const auto leftSize = row1_->childrenSize();
    if (pos < leftSize) {
      return row1_->childAt(pos);
    }
    VELOX_CHECK_LT(
        pos - leftSize,
        row2_->childrenSize(),
        "JoinedRowVector position {} out of range (arity={})",
        pos,
        leftSize + row2_->childrenSize());
    return row2_->childAt(pos - leftSize);
  }

  /// Returns true if the field at 'pos' is null for row 'rowIdx'. Mirrors
  /// Flink JoinedRowData::isNullAt(pos), but takes an explicit row index
  /// because a Velox RowVector holds a batch of rows.
  bool isNullAt(vector_size_t rowIdx, size_t pos) const {
    return childAt(pos)->isNullAt(rowIdx);
  }

  /// Returns the combined RowType for the two backing rows. Computed lazily
  /// and cached until the next replace() call.
  const RowTypePtr& type() const {
    ensureBacked();
    if (combinedType_ == nullptr) {
      combinedType_ = std::dynamic_pointer_cast<const RowType>(row1_->type())
                          ->unionWith(std::dynamic_pointer_cast<const RowType>(
                              row2_->type()));
    }
    return combinedType_;
  }

  /// Materializes this view as a regular RowVector by aliasing both backing
  /// rows' children into a new RowVector. No row data is copied: the new
  /// RowVector shares the underlying column buffers with row1 and row2.
  ///
  /// If the caller does not provide 'pool', the pool of row1 is used.
  RowVectorPtr toRowVector(memory::MemoryPool* pool = nullptr) const {
    ensureBacked();
    std::vector<VectorPtr> children;
    children.reserve(row1_->childrenSize() + row2_->childrenSize());
    for (size_t i = 0; i < row1_->childrenSize(); ++i) {
      children.push_back(row1_->childAt(i));
    }
    for (size_t i = 0; i < row2_->childrenSize(); ++i) {
      children.push_back(row2_->childAt(i));
    }
    return std::make_shared<RowVector>(
        pool != nullptr ? pool : row1_->pool(),
        type(),
        /*nulls=*/nullptr,
        row1_->size(),
        std::move(children));
  }

  /// Equality mirrors Flink JoinedRowData::equals: same rowKind, same row1
  /// and same row2. Per-row comparison is delegated to
  /// stateful::equalRowVectors which performs a null-safe, encoding-agnostic
  /// value comparison.
  bool equals(const JoinedRowVector& other) const {
    return rowKind_ == other.rowKind_ &&
        equalRowVectors(row1_, other.row1_) &&
        equalRowVectors(row2_, other.row2_);
  }

  bool operator==(const JoinedRowVector& other) const {
    return equals(other);
  }

  bool operator!=(const JoinedRowVector& other) const {
    return !equals(other);
  }

  /// Matches Flink JoinedRowData::toString format:
  /// "<shortString>{row1=..., row2=...}".
  std::string toString() const {
    std::string out{shortString(rowKind_)};
    if (row1_ == nullptr || row2_ == nullptr) {
      out.append("{<unset>}");
      return out;
    }
    out.append("{row1=")
        .append(row1_->toString())
        .append(", row2=")
        .append(row2_->toString())
        .append("}");
    return out;
  }

 private:
  void ensureBacked() const {
    VELOX_CHECK_NOT_NULL(
        row1_,
        "JoinedRowVector backing row1 is null; call replace() before access");
    VELOX_CHECK_NOT_NULL(
        row2_,
        "JoinedRowVector backing row2 is null; call replace() before access");
  }

  RowKind rowKind_{RowKind::INSERT};
  RowVectorPtr row1_;
  RowVectorPtr row2_;
  // Cached combined type, recomputed when backing rows change.
  mutable RowTypePtr combinedType_;
};

} // namespace facebook::velox::stateful
