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

#include "velox/core/ITypedExpr.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"
#include "velox/connectors/Connector.h"
#include "folly/dynamic.h"

namespace facebook::velox::connector::kafka {

using String = std::string;

class KafkaTableHandle : public ConnectorTableHandle {

public:
    KafkaTableHandle(
        String connectorId,
        String tableName,
        bool filterPushdownEnabled,
        common::SubfieldFilters subfieldFilters,
        const core::TypedExprPtr& remainingFilter,
        const RowTypePtr& dataColumns = nullptr,
        const RowTypePtr& projectedDataColumns = nullptr,
        const std::unordered_map<String, String>& tableParameters = {})
    : ConnectorTableHandle(connectorId)
    , tableName_(tableName)
    , filterPushdownEnabled_(filterPushdownEnabled)
    , subfieldFilters_(std::move(subfieldFilters))
    , remainingFilter_(remainingFilter)
    , dataColumns_(dataColumns)
    , projectedDataColumns_(projectedDataColumns)
    , tableParameters_(tableParameters) {}

    const String & tableName() {
        return tableName_;
    }

    const bool filterPushdownEnabled() {
        return filterPushdownEnabled_;
    }

    const common::SubfieldFilters & subfieldFilters() {
        return subfieldFilters_;
    }

    const core::TypedExprPtr & remainingFilter() {    
        return remainingFilter_;
    }

    const RowTypePtr & dataColumns() {
        return dataColumns_;
    }

    const RowTypePtr & projectedDataColumns() {
        return projectedDataColumns_;
    }

    const std::unordered_map<String, String> & tableParameters() {
        return tableParameters_;
    }

    std::unordered_map<String, String> & getTableParameters() {
        return tableParameters_;
    }

    const std::string& name() const override {
        return tableName_;
    }

    String toString() const override;

    folly::dynamic serialize() const override;

    static ConnectorTableHandlePtr create(const folly::dynamic& obj, void* context);

    static void registerSerDe();

private:
    String tableName_;
    bool filterPushdownEnabled_;
    common::SubfieldFilters subfieldFilters_;
    core::TypedExprPtr remainingFilter_;
    RowTypePtr dataColumns_;
    RowTypePtr projectedDataColumns_;
    std::unordered_map<String, String> tableParameters_;

};

}
