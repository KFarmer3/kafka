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

import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.TopicPartition;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeleteRecordsRequest extends AbstractRequest {
    
    public static final long HIGH_WATERMARK = -1L;

    public static class Builder extends AbstractRequest.Builder<DeleteRecordsRequest> {

        private final DeleteRecordsRequestData data;

        public Builder(DeleteRecordsRequestData data) {
            super(ApiKeys.DELETE_RECORDS);
            this.data = data;
        }

        @Override
        public DeleteRecordsRequest build(short version) {
            return new DeleteRecordsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DeleteRecordsRequestData data;

    public DeleteRecordsRequest(Struct struct, short version) {
        super(ApiKeys.DELETE_RECORDS, version);
        this.data = new DeleteRecordsRequestData(struct, version);
    }

    public DeleteRecordsRequest(DeleteRecordsRequestData data, short version) {
        super(ApiKeys.DELETE_RECORDS, version);
        this.data = data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        DeleteRecordsResponseData response = new DeleteRecordsResponseData();
        response.setThrottleTimeMs(throttleTimeMs);
        List<DeleteRecordsTopicResult> topics = new ArrayList<DeleteRecordsTopicResult>();
        
        for(DeleteRecordsTopic topic : data.topics()){
            
            DeleteRecordsTopicResult topicResult = new DeleteRecordsTopicResult();
            topicResult.setName(topic.name());
            List<DeleteRecordsPartitionResult> partitionsResult = new ArrayList<DeleteRecordsPartitionResult>();
            for(DeleteRecordsPartition partition : topic.partitions()){
                DeleteRecordsPartitionResult newPartition = new DeleteRecordsPartitionResult();
                newPartition.setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK);
                newPartition.setErrorCode(Errors.forException(e).code());
                newPartition.setPartitionIndex(partition.partitionIndex());
            }
            topicResult.setPartitions(partitionsResult);
            
        }
        
        response.setTopics(topics);

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new DeleteRecordsResponse(response);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ApiKeys.DELETE_RECORDS.latestVersion()));
        }
    }
    
    public Map<TopicPartition, Long> partitionOffsets(){
        Map<TopicPartition, Long> partitionOffsets = new HashMap<TopicPartition, Long>();
        for(DeleteRecordsTopic topic : data.topics()){
            for(DeleteRecordsPartition partition : topic.partitions()){
                TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
                partitionOffsets.put(topicPartition, partition.offset());
            }
        }
        return partitionOffsets;
        
    }

    public int timeout() {
        return data.timeoutMs();
    }

    public List<DeleteRecordsTopic> topics() {
        return data.topics();
    }

    public DeleteRecordsRequestData data() {
        return data;
    }

    public static DeleteRecordsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteRecordsRequest(ApiKeys.DELETE_RECORDS.parseRequest(version, buffer), version);
    }
}
