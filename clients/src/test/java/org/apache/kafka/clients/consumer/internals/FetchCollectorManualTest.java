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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
// import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
// import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
// import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
// import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.junit.jupiter.api.Assertions.assertFalse;
// import static org.junit.jupiter.api.Assertions.assertNotNull;
// import static org.junit.jupiter.api.Assertions.assertNull;
// import static org.junit.jupiter.api.Assertions.assertSame;
// import static org.junit.jupiter.api.Assertions.assertThrows;
// import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This tests the {@link FetchCollector} functionality in addition to what {@link FetcherTest} tests during the course
 * of its tests.
 */
public class FetchCollectorManualTest {

    private final static int DEFAULT_RECORD_COUNT = 10;
    private final static int DEFAULT_MAX_POLL_RECORDS = ConsumerConfig.DEFAULT_MAX_POLL_RECORDS;
    private final Time time = new MockTime(0, 0, 0);
    private final TopicPartition topicAPartition0 = new TopicPartition("topic-a", 0);
    private final TopicPartition topicAPartition1 = new TopicPartition("topic-a", 1);
    private final TopicPartition topicAPartition2 = new TopicPartition("topic-a", 2);
    private final Set<TopicPartition> allPartitions = partitions(topicAPartition0, topicAPartition1, topicAPartition2);
    private LogContext logContext;

    private SubscriptionState subscriptions;
    private FetchConfig fetchConfig;
    private FetchMetricsManager metricsManager;
    private ConsumerMetadata metadata;
    private FetchBuffer fetchBuffer;
    private Deserializers<String, String> deserializers;
    private FetchCollector<String, String> fetchCollector;
    private CompletedFetchBuilder completedFetchBuilder;
    private CompletedFetchBuilder1 completedFetchBuilder1;



    @Test
    public void testFetchWithMultipleCompletedFetchesExceedingMaxPollRecords() {
        int maxPollRecords = 10;
        int recordCount1 = maxPollRecords / 2 + 1; // 6 records
        int recordCount2 = maxPollRecords / 2 + 1; // 6 records
        buildDependencies(maxPollRecords);

        assign(topicAPartition0, topicAPartition1, topicAPartition2);

        CompletedFetch completedFetch1 = completedFetchBuilder
                .recordCount(recordCount1)
                .build();

        fetchBuffer.add(completedFetch1);

        CompletedFetch completedFetch2 = completedFetchBuilder1
                .recordCount(recordCount2)
                .build();

        fetchBuffer.add(completedFetch2);


        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);


        assertEquals(maxPollRecords, fetch.numRecords());
    }

    /**
     * This is a handy utility method for returning a set from a varargs array.
     */
    private static Set<TopicPartition> partitions(TopicPartition... partitions) {
        return new HashSet<>(Arrays.asList(partitions));
    }

    private void buildDependencies() {
        buildDependencies(DEFAULT_MAX_POLL_RECORDS);
    }

    private void buildDependencies(int maxPollRecords) {
        logContext = new LogContext();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));

        ConsumerConfig config = new ConsumerConfig(p);

        deserializers = new Deserializers<>(new StringDeserializer(), new StringDeserializer());

        subscriptions = createSubscriptionState(config, logContext);
        fetchConfig = new FetchConfig(config);

        Metrics metrics = createMetrics(config, time);
        metricsManager = createFetchMetricsManager(metrics);
        metadata = new ConsumerMetadata(
                0,
                1000,
                10000,
                false,
                false,
                subscriptions,
                logContext,
                new ClusterResourceListeners());
        fetchCollector = new FetchCollector<>(
                logContext,
                metadata,
                subscriptions,
                fetchConfig,
                deserializers,
                metricsManager,
                time);
        fetchBuffer = new FetchBuffer(logContext);
        completedFetchBuilder = new CompletedFetchBuilder();
        completedFetchBuilder1 = new CompletedFetchBuilder1();
    }

    private void assign(TopicPartition... partitions) {
        System.out.println(partitions(partitions));
        subscriptions.assignFromUser(partitions(partitions));
        for (TopicPartition partition : partitions) {
            subscriptions.seek(partition, 0);
        }
        
    }

    private void assignAndSeek(TopicPartition tp) {
        assign(tp);
        subscriptions.seek(tp, 0);
    }

    /**
     * Supplies the {@link Arguments} to {@link #testFetchWithMetadataRefreshErrors(Errors)}.
     */
    private static Stream<Arguments> testFetchWithMetadataRefreshErrorsSource() {
        List<Errors> errors = Arrays.asList(
                Errors.NOT_LEADER_OR_FOLLOWER,
                Errors.REPLICA_NOT_AVAILABLE,
                Errors.KAFKA_STORAGE_ERROR,
                Errors.FENCED_LEADER_EPOCH,
                Errors.OFFSET_NOT_AVAILABLE,
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                Errors.UNKNOWN_TOPIC_ID,
                Errors.INCONSISTENT_TOPIC_ID
        );

        return errors.stream().map(Arguments::of);
    }

    /**
     * Supplies the {@link Arguments} to {@link #testFetchWithOtherErrors(Errors)}.
     */
    private static Stream<Arguments> testFetchWithOtherErrorsSource() {
        List<Errors> errors = new ArrayList<>(Arrays.asList(Errors.values()));
        errors.removeAll(Arrays.asList(
                Errors.NONE,
                Errors.NOT_LEADER_OR_FOLLOWER,
                Errors.REPLICA_NOT_AVAILABLE,
                Errors.KAFKA_STORAGE_ERROR,
                Errors.FENCED_LEADER_EPOCH,
                Errors.OFFSET_NOT_AVAILABLE,
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                Errors.UNKNOWN_TOPIC_ID,
                Errors.INCONSISTENT_TOPIC_ID,
                Errors.OFFSET_OUT_OF_RANGE,
                Errors.TOPIC_AUTHORIZATION_FAILED,
                Errors.UNKNOWN_LEADER_EPOCH,
                Errors.UNKNOWN_SERVER_ERROR,
                Errors.CORRUPT_MESSAGE
        ));

        return errors.stream().map(Arguments::of);
    }


    /**
     * Supplies the {@link Arguments} to {@link #testErrorInInitialize(int, RuntimeException)}.
     */
    private static Stream<Arguments> testErrorInInitializeSource() {
        return Stream.of(
                Arguments.of(10, new RuntimeException()),
                Arguments.of(0, new RuntimeException()),
                Arguments.of(10, new KafkaException()),
                Arguments.of(0, new KafkaException())
        );
    }

    private class CompletedFetchBuilder {

        private long fetchOffset = 0;

        private int recordCount = DEFAULT_RECORD_COUNT;

        private Errors error = null;

        private CompletedFetchBuilder fetchOffset(long fetchOffset) {
            this.fetchOffset = fetchOffset;
            return this;
        }

        private CompletedFetchBuilder recordCount(int recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        private CompletedFetchBuilder error(Errors error) {
            this.error = error;
            return this;
        }

        private CompletedFetch build() {
            Records records;
            ByteBuffer allocate = ByteBuffer.allocate(1024);

            try (MemoryRecordsBuilder builder = MemoryRecords.builder(allocate,
                    CompressionType.NONE,
                    TimestampType.CREATE_TIME,
                    0)) {
                for (int i = 0; i < recordCount; i++)
                    builder.append(0L, "key".getBytes(), ("value-" + i).getBytes());

                records = builder.build();
            }

            FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                    .setPartitionIndex(topicAPartition0.partition())
                    .setHighWatermark(1000)
                    .setRecords(records);

            if (error != null)
                partitionData.setErrorCode(error.code());

            FetchMetricsAggregator metricsAggregator = new FetchMetricsAggregator(metricsManager, allPartitions);
            return new CompletedFetch(
                    logContext,
                    subscriptions,
                    BufferSupplier.create(),
                    topicAPartition0,
                    partitionData,
                    metricsAggregator,
                    fetchOffset,
                    ApiKeys.FETCH.latestVersion());
        }
    }

    private class CompletedFetchBuilder1 {

        private long fetchOffset = 0;

        private int recordCount = DEFAULT_RECORD_COUNT;

        private Errors error = null;

        private CompletedFetchBuilder1 fetchOffset(long fetchOffset) {
            this.fetchOffset = fetchOffset;
            return this;
        }

        private CompletedFetchBuilder1 recordCount(int recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        private CompletedFetchBuilder1 error(Errors error) {
            this.error = error;
            return this;
        }

        private CompletedFetch build() {
            Records records;
            ByteBuffer allocate = ByteBuffer.allocate(1024);

            try (MemoryRecordsBuilder builder = MemoryRecords.builder(allocate,
                    CompressionType.NONE,
                    TimestampType.CREATE_TIME,
                    0)) {
                for (int i = 0; i < recordCount; i++)
                    builder.append(0L, "key".getBytes(), ("value-" + i).getBytes());

                records = builder.build();
            }

            FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                    .setPartitionIndex(topicAPartition1.partition())
                    .setHighWatermark(1000)
                    .setRecords(records);

            if (error != null)
                partitionData.setErrorCode(error.code());

            FetchMetricsAggregator metricsAggregator = new FetchMetricsAggregator(metricsManager, allPartitions);
            return new CompletedFetch(
                    logContext,
                    subscriptions,
                    BufferSupplier.create(),
                    topicAPartition1,
                    partitionData,
                    metricsAggregator,
                    fetchOffset,
                    ApiKeys.FETCH.latestVersion());
        }
    }

    private class CompletedFetchBuilder2 {

        private long fetchOffset = 0;

        private int recordCount = DEFAULT_RECORD_COUNT;

        private Errors error = null;

        private CompletedFetchBuilder2 fetchOffset(long fetchOffset) {
            this.fetchOffset = fetchOffset;
            return this;
        }

        private CompletedFetchBuilder2 recordCount(int recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        private CompletedFetchBuilder2 error(Errors error) {
            this.error = error;
            return this;
        }

        private CompletedFetch build() {
            Records records;
            ByteBuffer allocate = ByteBuffer.allocate(1024);

            try (MemoryRecordsBuilder builder = MemoryRecords.builder(allocate,
                    CompressionType.NONE,
                    TimestampType.CREATE_TIME,
                    0)) {
                for (int i = 0; i < recordCount; i++)
                    builder.append(0L, "key".getBytes(), ("value-" + i).getBytes());

                records = builder.build();
            }

            FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                    .setPartitionIndex(topicAPartition2.partition())
                    .setHighWatermark(1000)
                    .setRecords(records);

            if (error != null)
                partitionData.setErrorCode(error.code());

            FetchMetricsAggregator metricsAggregator = new FetchMetricsAggregator(metricsManager, allPartitions);
            return new CompletedFetch(
                    logContext,
                    subscriptions,
                    BufferSupplier.create(),
                    topicAPartition2,
                    partitionData,
                    metricsAggregator,
                    fetchOffset,
                    ApiKeys.FETCH.latestVersion());
        }
    }
}