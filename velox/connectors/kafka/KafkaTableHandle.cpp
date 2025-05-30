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

#include "velox/connectors/kafka/KafkaTableHandle.h"

namespace facebook::velox::connector::kafka {

    String KafkaTableHandle::toString() const{
        std::stringstream out;
        out << "table: " << tableName_;
        if (!subfieldFilters_.empty()) {
            // Sort filters by subfield for deterministic output.
            std::map<std::string, common::Filter*> orderedFilters;
            for (const auto& [field, filter] : subfieldFilters_) {
                orderedFilters[field.toString()] = filter.get();
            }
            out << ", range filters: [";
            bool notFirstFilter = false;
            for (const auto& [field, filter] : orderedFilters) {
                if (notFirstFilter) {
                    out << ", ";
                }
                out << "(" << field << ", " << filter->toString() << ")";
                notFirstFilter = true;
            }
            out << "]";
        }
        if (remainingFilter_) {
            out << ", remaining filter: (" << remainingFilter_->toString() << ")";
        }
        if (dataColumns_) {
            out << ", data columns: " << dataColumns_->toString();
        }
        if (projectedDataColumns_) {
            out << ", projected data columns: " << projectedDataColumns_->toString();
        }
        if (!tableParameters_.empty()) {
            std::map<std::string, std::string> orderedTableParameters{tableParameters_.begin(), tableParameters_.end()};
            out << ", table parameters: [";
            bool firstParam = true;
            for (const auto& param : orderedTableParameters) {
                if (!firstParam) {
                    out << ", ";
                }
                out << param.first << ":" << param.second;
                firstParam = false;
            }
            out << "]";
        }
        return out.str();
    }

    folly::dynamic KafkaTableHandle::serialize() const {
        folly::dynamic obj = ConnectorTableHandle::serializeBase("KafkaTableHandle");
        obj["tableName"] = tableName_;
        obj["filterPushdownEnabled"] = filterPushdownEnabled_;
        folly::dynamic subfieldFilters = folly::dynamic::array;
        for (const auto& [subfield, filter] : subfieldFilters_) {
            folly::dynamic pair = folly::dynamic::object;
            pair["subfield"] = subfield.toString();
            pair["filter"] = filter->serialize();
            subfieldFilters.push_back(pair);
        }

        obj["subfieldFilters"] = subfieldFilters;
        if (remainingFilter_) {
            obj["remainingFilter"] = remainingFilter_->serialize();
        }
        if (dataColumns_) {
            obj["dataColumns"] = dataColumns_->serialize();
        }
        if (projectedDataColumns_) {
            obj["projectedDataColumns"] = projectedDataColumns_->serialize();
        }
        folly::dynamic tableParameters = folly::dynamic::object;
        for (const auto& param : tableParameters_) {
            tableParameters[param.first] = param.second;
        }
        obj["tableParameters"] = tableParameters;
        return obj;
    }

    ConnectorTableHandlePtr KafkaTableHandle::create(const folly::dynamic & obj, void* context) {
        auto connectorId = obj["connectorId"].asString();
        auto tableName = obj["tableName"].asString();
        auto filterPushdownEnabled = obj["filterPushdownEnabled"].asBool();

        core::TypedExprPtr remainingFilter;
        if (auto it = obj.find("remainingFilter"); it != obj.items().end()) {
            remainingFilter =
                ISerializable::deserialize<core::ITypedExpr>(it->second, context);
        }

        common::SubfieldFilters subfieldFilters;
        folly::dynamic subfieldFiltersObj = obj["subfieldFilters"];
        for (const auto& subfieldFilter : subfieldFiltersObj) {
            common::Subfield subfield(subfieldFilter["subfield"].asString());
            auto filter =ISerializable::deserialize<common::Filter>(subfieldFilter["filter"]);
            subfieldFilters[common::Subfield(std::move(subfield.path()))] = filter->clone();
        }

        RowTypePtr dataColumns;
        if (auto it = obj.find("dataColumns"); it != obj.items().end()) {
            dataColumns = ISerializable::deserialize<RowType>(it->second, context);
        }

        RowTypePtr projectedDataColumns;
        if (auto it = obj.find("projectedDataColumns"); it != obj.items().end()) {
            projectedDataColumns = ISerializable::deserialize<RowType>(it->second, context);
        }

        std::unordered_map<std::string, std::string> tableParameters{};
        const auto& tableParametersObj = obj["tableParameters"];
        for (const auto& key : tableParametersObj.keys()) {
            const auto& value = tableParametersObj[key];
            tableParameters.emplace(key.asString(), value.asString());
        }

        return std::make_shared<const KafkaTableHandle>(
            connectorId,
            tableName,
            filterPushdownEnabled,
            std::move(subfieldFilters),
            remainingFilter,
            dataColumns,
            projectedDataColumns,
            tableParameters);
    }

    void KafkaTableHandle::registerSerDe() {
        auto& registry = DeserializationWithContextRegistryForSharedPtr();
        registry.Register("KafkaTableHandle", create);
    }
}