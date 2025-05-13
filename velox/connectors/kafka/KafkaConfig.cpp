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

#include "velox/connectors/kafka/KafkaConfig.h"

namespace facebook::velox::connector::kafka {

    template<typename T, bool throwException>
    const T KafkaConfig::checkAndGetConfigValue(const String & configKey, T defaultValue) const {
        std::optional<T> configValue = static_cast<std::optional<T>>(config_->get<T>(configKey));
        if constexpr (throwException) {
            VELOX_CHECK_EQ(configValue.has_value(), true, "Kafka config {} has no specified value.", configKey);
        }
        if (configValue.has_value()) {
            return configValue.value();
        } else {
            return defaultValue;
        }
    }

    const String ConnectionConfig::getBootstrapServers() const {
        return checkAndGetConfigValue<String, true>(kBootstrapServers, "");
    }

    const String ConnectionConfig::getTopic() const {
        return checkAndGetConfigValue<String, true>(kTopic, "");
    }

    const String ConnectionConfig::getGroupId() const {
        return checkAndGetConfigValue<String, true>(kGroupId, "");
    }

    const String ConnectionConfig::getClientId() const {
        return checkAndGetConfigValue<String, true>(kClientId, "");
    }

    const String ConnectionConfig::getFormat() const {
        return checkAndGetConfigValue<String, true>(kFormat, "");
    }

    const String ConnectionConfig::getAutoOffsetReset() const {
        return checkAndGetConfigValue<String, false>(kAutoResetOffset, defaultAutoOffsetRest);
    }

    const uint32_t ConnectionConfig::getQueuedMinMessages() const {
        return checkAndGetConfigValue<uint32_t, false>(kQueueMinMessages, defaultQueuedMinMessages);
    }

    const bool ConnectionConfig::getEnableAutoCommit() const {
        return checkAndGetConfigValue<String, false>(kEnableAutoCommit, "true") == "true" ? true : false;
    }

    const bool ConnectionConfig::getEnablePartitionEof() const {
        return checkAndGetConfigValue<bool, false>(kEnablePartitionEof, false);
    }

    const uint32_t ConnectionConfig::getConsumeQueueSize() const {
        return checkAndGetConfigValue<uint32_t, false>(kConsumeMessageQueueSize, defaultConsumeMessageQueueSize);
    }

    const uint32_t ConnectionConfig::getPollMaxBatchSize() const {
        return checkAndGetConfigValue<uint32_t, false>(kPollMaxBatchSize, defaultPollMaxBatchSize);
    }
    
    const uint32_t ConnectionConfig::getPollTimeoutMills() const {
        return checkAndGetConfigValue<uint32_t, false>(kPollTimeoutMills, defaultPollTimeoutMills);
    }

    const String ConnectionConfig::getStartupMode() const {
        return checkAndGetConfigValue<String, false>(kStartupMode, defaultConsumeStartupMode);
    }

    cppkafka::Configuration ConnectionConfig::getCppKafkaConfiguration() const {
        cppkafka::Configuration conf;
        conf.set("metadata.broker.list", getBootstrapServers());
        conf.set("group.id", getGroupId());
        conf.set("client.id", getClientId());
        conf.set("client.software.name", defaultClientSoftwareName);
        conf.set("client.software.version", defaultClientSoftwareVersion);
        conf.set("auto.offset.reset", getAutoOffsetReset()); 
        conf.set("queued.min.messages", getQueuedMinMessages());
        conf.set("enable.auto.commit", getEnableAutoCommit());
        conf.set("enable.partition.eof", getEnablePartitionEof()); // Ignore EOF messages
        return conf;
    }
}