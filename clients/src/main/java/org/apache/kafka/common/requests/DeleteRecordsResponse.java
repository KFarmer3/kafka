/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT64;

public class DeleteRecordsResponse extends AbstractResponse {
    
    public static final long INVALID_LOW_WATERMARK = -1L;

    private final DeleteRecordsResponseData data;

    /**
     * Possible error code:
     *
     * OFFSET_OUT_OF_RANGE (1)
     * UNKNOWN_TOPIC_OR_PARTITION (3)
     * NOT_LEADER_FOR_PARTITION (6)
     * REQUEST_TIMED_OUT (7)
     * UNKNOWN (-1)
     */

    public static final class PartitionResponse {
        public long lowWatermark;
        public Errors error;

        public PartitionResponse(long lowWatermark, Errors error) {
            this.lowWatermark = lowWatermark;
            this.error = error;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append('{')
                   .append(",low_watermark: ")
                   .append(lowWatermark)
                   .append("error: ")
                   .append(error.toString())
                   .append('}');
            return builder.toString();
        }
    }

    public DeleteRecordsResponse(Struct struct, short version) {
        this.data = new DeleteRecordsResponseData(struct, version);
    }

    /**
     * Constructor for version 0.
     */
    public DeleteRecordsResponse(DeleteRecordsResponseData data) {
        this.data = data;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }
    
    public List<DeleteRecordsTopicResult> topics() {
        return data.topics();
    }

    //Still need to work out if this is needed?
    
    public Map<TopicPartition, PartitionResponse> responses() {
        Map<TopicPartition, PartitionResponse> topicMap = new HashMap<TopicPartition, PartitionResponse>();
        for(DeleteRecordsTopicResult topic : data.topics()){
            for(DeleteRecordsPartitionResult partition : topic.partitions()) {
                TopicPartition newTopic = new TopicPartition(topic.name(), partition.partitionIndex());
                PartitionResponse newPartition = new PartitionResponse(partition.lowWatermark(), Errors.forCode(partition.errorCode()));
                topicMap.put(newTopic, newPartition);
            }
        }
        return topicMap;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<Errors, Integer>();
        for (DeleteRecordsTopicResult topic : data.topics()){
            for (DeleteRecordsPartitionResult partition : topic.partitions()){
                errorCounts.put(Errors.forCode(partition.errorCode()), 1);
            }
        }
        return errorCounts;
    }

    public static DeleteRecordsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteRecordsResponse(ApiKeys.DELETE_RECORDS.responseSchema(version).read(buffer), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
