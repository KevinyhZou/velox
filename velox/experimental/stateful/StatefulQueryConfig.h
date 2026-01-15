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

#include "velox/experimental/stateful/state/StateBackend.h"
#include "velox/core/QueryConfig.h"

namespace facebook::velox::stateful {

class StatefulQueryConfig : public core::QueryConfig {
public:
    explicit StatefulQueryConfig(const std::unordered_map<std::string, std::string>& values)
        : core::QueryConfig(values) {}
    
    explicit StatefulQueryConfig(std::unordered_map<std::string, std::string>&& values)
        : core::QueryConfig(std::move(values)) {}

    static constexpr const char* kStateBackendType = "state_backend";
    static constexpr const char* kRocksDBStateBackendNativeHandle = "state_backend_rocksdb_native_handle";

    StateBackendType stateBackendType() const {
        int32_t backendType = get<int32_t>(kStateBackendType, 0);
        return static_cast<StateBackendType>(backendType);
    }

    int64_t rocksdbStateBackendHandle() const {
        return get<int64_t>(kRocksDBStateBackendNativeHandle, -1);
    }


};
}
