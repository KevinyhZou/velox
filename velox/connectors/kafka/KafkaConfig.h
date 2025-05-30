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

#include "velox/common/config/Config.h"
#include "cppkafka/cppkafka.h"
#include <optional>

namespace facebook::velox::connector::kafka {

using ConfigPtr = std::shared_ptr<const config::ConfigBase>;
using String = std::string;

class KafkaConfig {

public:
    KafkaConfig(const ConfigPtr & config) : config_(config) {}

    const ConfigPtr & getConfig() const {
        return config_;
    }

    const bool exists(const String & configKey) const {
        return config_ && config_->valueExists(configKey);
    }

    const bool empty() const {
        if (!config_) {
            return true;
        } else {
            return config_->rawConfigs().empty();
        }
    }

    template<typename T>
    const std::shared_ptr<T> setConfigs(const std::unordered_map<String, String> & configs) const {
        std::unordered_map<String, String> rawConfigs = config_->rawConfigsCopy();
        rawConfigs.insert(configs.begin(), configs.end());
        ConfigPtr newConfig = std::make_shared<const config::ConfigBase>(std::move(rawConfigs));
        return std::make_shared<T>(newConfig);
    }

protected:
    ConfigPtr config_;
    template<typename T, bool throwException>
    const T checkAndGetConfigValue(const String & configKey, T defaultValue) const;

};

class ConnectionConfig : public KafkaConfig {
public:
    ConnectionConfig(const ConfigPtr & config) : KafkaConfig(config) {}
    /// The config key of bootstrap servers
    static constexpr const char* kBootstrapServers = "bootstrap.servers";
    /// The config key of topic
    static constexpr const char* kTopic = "topic";
    /// The config key of group id
    static constexpr const char* kGroupId = "group.id";
    /// The config key of client id
    static constexpr const char* kClientId = "client.id";
    /// The config key fo format
    static constexpr const char* kFormat = "format";
    /// The config key of auto offset reset
    static constexpr const char* kAutoResetOffset = "auto.offset.reset";
    /// The config key of minimum number of messages of the queue buffer
    static constexpr const char* kQueueMinMessages = "queued.min.messages";
    /// The config key of whether to enable auto commit kafka coffset
    static constexpr const char* kEnableAutoCommit = "enable.auto.commit";
    /// The config key of whether to ignore partition eof
    static constexpr const char* kEnablePartitionEof = "enable.partition.eof";
    /// The config key of max batch size to poll kafka messages.
    static constexpr const char* kPollMaxBatchSize = "poll.max.batch.size";
    /// The config key of timeout milliseconds to poll kafka messages.
    static constexpr const char* kPollTimeoutMills = "poll.timeout.mills";
    /// The config key of queue buffer size of cppkafka client
    static constexpr const char* kConsumeMessageQueueSize = "consume.queue.size";
    /// The startup mode of kafka consumer, its value canbe `group-offsets`, `latest-offsets`, `earliest-offsets`, `timestamp`.
    static constexpr const char* kStartupMode = "scan.startup.mode";
    static constexpr const char* kProcessDataByBatch = "enable.batch.process.data";
    static constexpr const uint32_t defaultQueuedMinMessages = 1000000;
    static constexpr const char* defaultClientSoftwareName = "velox";
    static constexpr const char* defaultClientSoftwareVersion  = "***";
    static constexpr const uint32_t defaultPollMaxBatchSize = 100;
    static constexpr const uint32_t defaultPollTimeoutMills = 100;
    static constexpr const char* defaultConsumeStartupMode = "group-offsets";
    static constexpr const char* defaultAutoOffsetRest = "latest";

    const String getBootstrapServers() const;

    const String getTopic() const;

    const String getGroupId() const;

    const String getClientId() const;

    const String getFormat() const;

    const String getAutoOffsetReset() const;

    const uint32_t getQueuedMinMessages() const;

    const bool getEnableAutoCommit() const;

    const bool getEnablePartitionEof() const;

    const uint32_t getPollMaxBatchSize() const;

    const uint32_t getPollTimeoutMills() const;

    const uint32_t getConsumeQueueSize() const;

    const String getStartupMode() const;
    
    const bool getEnableBatchProcessData() const;

    cppkafka::Configuration getCppKafkaConfiguration() const;
};

class JSONFormatConfig : public KafkaConfig {
public:
    JSONFormatConfig(const ConfigPtr & config) : KafkaConfig(config) {}
    
};

class CSVFormatConfig : public KafkaConfig {

};

using KafkaConfigPtr = std::shared_ptr<KafkaConfig>;
using ConnectionConfigPtr = std::shared_ptr<ConnectionConfig>;
using JSONFormatConfigPtr = std::shared_ptr<JSONFormatConfig>;
using CSVFormatConfigPtr = std::shared_ptr<CSVFormatConfig>;

} // namespace facebook::velox::connector::kafka