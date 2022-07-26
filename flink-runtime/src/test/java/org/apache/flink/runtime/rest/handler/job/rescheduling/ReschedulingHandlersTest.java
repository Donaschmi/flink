/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job.rescheduling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.UnknownOperationKeyException;
import org.apache.flink.runtime.rest.RestMatchers;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.rescheduling.ReschedulingInfo;
import org.apache.flink.runtime.rest.messages.job.rescheduling.ReschedulingStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.rescheduling.ReschedulingTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.rescheduling.ReschedulingTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.rest.handler.job.rescheduling.ReschedulingTestUtilities.getResultIfKeyMatches;
import static org.apache.flink.runtime.rest.handler.job.rescheduling.ReschedulingTestUtilities.setReferenceToOperationKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Test for {@link ReschedulingHandlers}. */
public class ReschedulingHandlersTest extends TestLogger {

    private static final Time TIMEOUT = Time.seconds(10);

    private static final JobID JOB_ID = new JobID();

    private ReschedulingHandlers.ReschedulingTriggerHandler reschedulingTriggerHandler;

    private ReschedulingHandlers.ReschedulingStatusHandler reschedulingStatusHandler;

    @Before
    public void setUp() throws Exception {
        GatewayRetriever<RestfulGateway> leaderRetriever =
                () -> CompletableFuture.completedFuture(null);

        final ReschedulingHandlers reschedulingHandlers = new ReschedulingHandlers();
        reschedulingTriggerHandler =
                reschedulingHandlers
                .new ReschedulingTriggerHandler(leaderRetriever, TIMEOUT, Collections.emptyMap());

        reschedulingStatusHandler =
                new ReschedulingHandlers.ReschedulingStatusHandler(
                        leaderRetriever, TIMEOUT, Collections.emptyMap());
    }

    @Test
    public void testReschedulingCompletedSuccessfully() throws Exception {
        final OperationResult<String> successfulResult =
                OperationResult.success("COMPLETED_SAVEPOINT_EXTERNAL_POINTER");
        final AtomicReference<AsynchronousJobOperationKey> keyReference = new AtomicReference<>();
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerReschedulingFunction(setReferenceToOperationKey(keyReference))
                        .setGetReschedulingStatusFunction(
                                getResultIfKeyMatches(successfulResult, keyReference))
                        .build();

        final TriggerId triggerId =
                reschedulingTriggerHandler
                        .handleRequest(triggerReschedulingRequest(), testingRestfulGateway)
                        .get()
                        .getTriggerId();

        AsynchronousOperationResult<ReschedulingInfo> reschedulingResponseBody;
        reschedulingResponseBody =
                reschedulingStatusHandler
                        .handleRequest(reschedulingStatusRequest(triggerId), testingRestfulGateway)
                        .get();

        assertThat(
                reschedulingResponseBody.queueStatus().getId(), equalTo(QueueStatus.Id.COMPLETED));
        assertThat(reschedulingResponseBody.resource(), notNullValue());
    }

    @Test
    public void testProvidedTriggerId() throws Exception {
        final OperationResult<String> successfulResult =
                OperationResult.success("COMPLETED_SAVEPOINT_EXTERNAL_POINTER");
        AtomicReference<AsynchronousJobOperationKey> keyReference = new AtomicReference<>();
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerReschedulingFunction(setReferenceToOperationKey(keyReference))
                        .setGetReschedulingStatusFunction(
                                getResultIfKeyMatches(successfulResult, keyReference))
                        .build();

        final TriggerId providedTriggerId = new TriggerId();

        final TriggerId returnedTriggerId =
                reschedulingTriggerHandler
                        .handleRequest(
                                triggerReschedulingRequest(providedTriggerId),
                                testingRestfulGateway)
                        .get()
                        .getTriggerId();

        assertEquals(providedTriggerId, returnedTriggerId);

        AsynchronousOperationResult<ReschedulingInfo> reschedulingResponseBody;
        reschedulingResponseBody =
                reschedulingStatusHandler
                        .handleRequest(
                                reschedulingStatusRequest(providedTriggerId), testingRestfulGateway)
                        .get();

        assertThat(
                reschedulingResponseBody.queueStatus().getId(), equalTo(QueueStatus.Id.COMPLETED));
        assertThat(reschedulingResponseBody.resource(), notNullValue());
    }

    @Test
    public void testQueryStatusOfUnknownOperationReturnsError()
            throws HandlerRequestException, RestHandlerException {
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setGetReschedulingStatusFunction(
                                key ->
                                        FutureUtils.completedExceptionally(
                                                new UnknownOperationKeyException(key)))
                        .build();

        final CompletableFuture<AsynchronousOperationResult<ReschedulingInfo>> statusFuture =
                reschedulingStatusHandler.handleRequest(
                        reschedulingStatusRequest(new TriggerId()), testingRestfulGateway);

        assertThat(statusFuture, RestMatchers.respondsWithError(HttpResponseStatus.NOT_FOUND));
    }

    private static HandlerRequest<ReschedulingTriggerRequestBody> triggerReschedulingRequest()
            throws HandlerRequestException {
        return triggerReschedulingRequest(null);
    }

    private static HandlerRequest<ReschedulingTriggerRequestBody> triggerReschedulingRequest(
            @Nullable TriggerId triggerId) throws HandlerRequestException {
        return HandlerRequest.resolveParametersAndCreate(
                new ReschedulingTriggerRequestBody(triggerId),
                new ReschedulingTriggerMessageParameters(),
                Collections.singletonMap(JobIDPathParameter.KEY, JOB_ID.toString()),
                Collections.emptyMap(),
                Collections.emptyList());
    }

    private static HandlerRequest<EmptyRequestBody> reschedulingStatusRequest(
            final TriggerId triggerId) throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, JOB_ID.toString());
        pathParameters.put(TriggerIdPathParameter.KEY, triggerId.toString());

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new ReschedulingStatusMessageParameters(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
