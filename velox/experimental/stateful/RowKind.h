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

#include <cstdint>
#include <ostream>
#include <string_view>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::stateful {

/// This enum mirrors Flink org.apache.flink.types.RowKind. It lists all kinds
/// of changes that a row can describe in a changelog.
///
/// The underlying type and byte values are fixed (and match Flink) so they
/// are stable across versions and can be used directly for serialization.
enum class RowKind : uint8_t {
  /// Insertion operation.
  INSERT = 0,

  /// Update operation with the previous content of the updated row.
  ///
  /// This kind SHOULD occur together with UPDATE_AFTER for modelling an
  /// update that needs to retract the previous row first. It is useful in
  /// cases of a non-idempotent update, i.e., an update of a row that is not
  /// uniquely identifiable by a key.
  UPDATE_BEFORE = 1,

  /// Update operation with the new content of the updated row.
  ///
  /// This kind CAN occur together with UPDATE_BEFORE for modelling an update
  /// that needs to retract the previous row first. OR it describes an
  /// idempotent update, i.e., an update of a row that is uniquely
  /// identifiable by a key.
  UPDATE_AFTER = 2,

  /// Deletion operation.
  DELETE = 3,
};

/// Returns a short string representation of 'kind' (matches Flink):
///   INSERT        -> "+I"
///   UPDATE_BEFORE -> "-U"
///   UPDATE_AFTER  -> "+U"
///   DELETE        -> "-D"
inline std::string_view shortString(RowKind kind) {
  switch (kind) {
    case RowKind::INSERT:
      return "+I";
    case RowKind::UPDATE_BEFORE:
      return "-U";
    case RowKind::UPDATE_AFTER:
      return "+U";
    case RowKind::DELETE:
      return "-D";
  }
  VELOX_UNREACHABLE();
}

/// Returns the full uppercase name of 'kind' (matches Flink's enum name).
inline std::string_view name(RowKind kind) {
  switch (kind) {
    case RowKind::INSERT:
      return "INSERT";
    case RowKind::UPDATE_BEFORE:
      return "UPDATE_BEFORE";
    case RowKind::UPDATE_AFTER:
      return "UPDATE_AFTER";
    case RowKind::DELETE:
      return "DELETE";
  }
  VELOX_UNREACHABLE();
}

/// Returns the byte value representation of 'kind'. Mirrors Flink's
/// RowKind::toByteValue() and is used for serialization / deserialization.
inline uint8_t toByteValue(RowKind kind) {
  return static_cast<uint8_t>(kind);
}

/// Creates a RowKind from the given byte value. Mirrors Flink's
/// RowKind::fromByteValue(). Throws on unknown values.
inline RowKind fromByteValue(uint8_t value) {
  switch (value) {
    case 0:
      return RowKind::INSERT;
    case 1:
      return RowKind::UPDATE_BEFORE;
    case 2:
      return RowKind::UPDATE_AFTER;
    case 3:
      return RowKind::DELETE;
    default:
      VELOX_USER_FAIL("Unsupported byte value '{}' for row kind.", value);
  }
}

/// Returns true if 'kind' represents a deletion or an UPDATE_BEFORE (i.e. a
/// message that retracts a previous row). Convenience helper that does not
/// exist in Flink but is useful for streaming operators.
inline bool isRetract(RowKind kind) {
  return kind == RowKind::UPDATE_BEFORE || kind == RowKind::DELETE;
}

/// Returns true if 'kind' represents an insertion or an UPDATE_AFTER (i.e. a
/// message that introduces a new row value).
inline bool isAccumulate(RowKind kind) {
  return kind == RowKind::INSERT || kind == RowKind::UPDATE_AFTER;
}

inline std::ostream& operator<<(std::ostream& os, RowKind kind) {
  return os << name(kind);
}

} // namespace facebook::velox::stateful
