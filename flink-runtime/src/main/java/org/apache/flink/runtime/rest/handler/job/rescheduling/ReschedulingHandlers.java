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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.runtime.clusterframework.types.ReschedulePlanJSONMapper;
import org.apache.flink.runtime.dispatcher.UnknownOperationKeyException;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.rescheduling.ReschedulingInfo;
import org.apache.flink.runtime.rest.messages.job.rescheduling.ReschedulingStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.rescheduling.ReschedulingStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.rescheduling.ReschedulingTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.rescheduling.ReschedulingTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.rescheduling.ReschedulingTriggerRequestBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * HTTP handlers for asynchronous triggering of rescheduling.
 *
 * <p>Drawing savepoints is a potentially long-running operation. To avoid blocking HTTP
 * connections, savepoints must be drawn in two steps. First, an HTTP request is issued to trigger
 * the savepoint asynchronously. The request will be assigned a {@link TriggerId}, which is returned
 * in the response body. Next, the returned id should be used to poll the status of the savepoint
 * until it is finished.
 *
 * <p>A savepoint is triggered by sending an HTTP {@code POST} request to {@code
 * /jobs/:jobid/savepoints}. The HTTP request may contain a JSON body to specify the target
 * directory of the savepoint, e.g.,
 *
 * <pre>
 * { "target-directory": "/tmp" }
 * </pre>
 *
 * <p>If the body is omitted, or the field {@code target-property} is {@code null}, the default
 * savepoint directory as specified by {@link CheckpointingOptions#SAVEPOINT_DIRECTORY} will be
 * used. As written above, the response will contain a request id, e.g.,
 *
 * <pre>
 * { "request-id": "7d273f5a62eb4730b9dea8e833733c1e" }
 * </pre>
 *
 * <p>To poll for the status of an ongoing savepoint, an HTTP {@code GET} request is issued to
 * {@code /jobs/:jobid/savepoints/:savepointtriggerid}. If the specified savepoint is still ongoing,
 * the response will be
 *
 * <pre>
 * {
 *     "status": {
 *         "id": "IN_PROGRESS"
 *     }
 * }
 * </pre>
 *
 * <p>If the specified savepoint has completed, the status id will transition to {@code COMPLETED},
 * and the response will additionally contain information about the savepoint, such as the location:
 *
 * <pre>
 * {
 *     "status": {
 *         "id": "COMPLETED"
 *     },
 *     "operation": {
 *         "location": "/tmp/savepoint-d9813b-8a68e674325b"
 *     }
 * }
 * </pre>
 */
public class ReschedulingHandlers {

    private abstract static class ReschedulingHandlerBase<B extends RequestBody>
            extends AbstractRestHandler<
                    RestfulGateway, B, TriggerResponse, ReschedulingTriggerMessageParameters> {

        ReschedulingHandlerBase(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                final MessageHeaders<B, TriggerResponse, ReschedulingTriggerMessageParameters>
                        messageHeaders) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        }

        protected AsynchronousJobOperationKey createOperationKey(final HandlerRequest<B> request) {
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            return AsynchronousJobOperationKey.of(
                    extractTriggerId(request.getRequestBody()).orElseGet(TriggerId::new), jobId);
        }

        protected abstract Optional<TriggerId> extractTriggerId(B request);

        public CompletableFuture<TriggerResponse> handleRequest(
                @Nonnull HandlerRequest<B> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            final AsynchronousJobOperationKey operationKey = createOperationKey(request);

            return triggerOperation(request, operationKey, gateway)
                    .handle(
                            (acknowledge, throwable) -> {
                                if (throwable == null) {
                                    return new TriggerResponse(operationKey.getTriggerId());
                                } else {
                                    throw new CompletionException(
                                            createInternalServerError(
                                                    throwable, operationKey, "triggering"));
                                }
                            });
        }

        protected abstract CompletableFuture<Acknowledge> triggerOperation(
                HandlerRequest<B> request,
                AsynchronousJobOperationKey operationKey,
                RestfulGateway gateway)
                throws RestHandlerException;
    }

    /** HTTP handler to trigger rescheduling. */
    public class ReschedulingTriggerHandler
            extends ReschedulingHandlerBase<ReschedulingTriggerRequestBody> {

        public ReschedulingTriggerHandler(
                final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                final Time timeout,
                final Map<String, String> responseHeaders) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    ReschedulingTriggerHeaders.getInstance());
        }

        @Override
        protected Optional<TriggerId> extractTriggerId(ReschedulingTriggerRequestBody request) {
            return request.getTriggerId();
        }

        @Override
        protected CompletableFuture<Acknowledge> triggerOperation(
                HandlerRequest<ReschedulingTriggerRequestBody> request,
                AsynchronousJobOperationKey operationKey,
                RestfulGateway gateway)
                throws RestHandlerException {
            final ReschedulePlanJSONMapper[] reschedulePlan =
                    request.getRequestBody().getReschedulePlan();
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            return gateway.triggerRescheduling(jobId, reschedulePlan, RpcUtils.INF_TIMEOUT);
        }
    }

    /** HTTP handler to query for the status of the rescheduling. */
    public static class ReschedulingStatusHandler
            extends AbstractRestHandler<
                    RestfulGateway,
                    EmptyRequestBody,
                    AsynchronousOperationResult<ReschedulingInfo>,
                    ReschedulingStatusMessageParameters> {

        public ReschedulingStatusHandler(
                final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                final Time timeout,
                final Map<String, String> responseHeaders) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    ReschedulingStatusHeaders.getInstance());
        }

        @Override
        protected CompletableFuture<AsynchronousOperationResult<ReschedulingInfo>> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {

            final AsynchronousJobOperationKey key = getOperationKey(request);

            return gateway.getTriggeredReschedulingStatus(key)
                    .handle(
                            (operationResult, throwable) -> {
                                return AsynchronousOperationResult.completed(
                                        operationResultResponse());
                                /*
                                if (throwable == null) {
                                    switch (operationResult.getStatus()) {
                                        case SUCCESS:
                                            return AsynchronousOperationResult.completed(
                                                    operationResultResponse());
                                        case FAILURE:
                                            return AsynchronousOperationResult.completed(
                                                    exceptionalOperationResultResponse(
                                                            operationResult.getThrowable()));
                                        case IN_PROGRESS:
                                            return AsynchronousOperationResult.inProgress();
                                        default:
                                            throw new IllegalStateException(
                                                    "No handler for operation status "
                                                            + operationResult.getStatus()
                                                            + ", encountered for key "
                                                            + key);
                                    }
                                } else {
                                    throw new CompletionException(
                                            maybeCreateNotFoundError(throwable, key)
                                                    .orElseGet(
                                                            () ->
                                                                    createInternalServerError(
                                                                            throwable,
                                                                            key,
                                                                            "retrieving status of")));
                                }*/
                            });
        }

        protected ReschedulingInfo exceptionalOperationResultResponse(Throwable throwable) {
            return new ReschedulingInfo(new SerializedThrowable(throwable));
        }

        protected ReschedulingInfo operationResultResponse() {
            return new ReschedulingInfo(null);
        }

        private static Optional<RestHandlerException> maybeCreateNotFoundError(
                Throwable throwable, AsynchronousJobOperationKey key) {
            if (ExceptionUtils.findThrowable(throwable, UnknownOperationKeyException.class)
                    .isPresent()) {
                return Optional.of(
                        new RestHandlerException(
                                String.format(
                                        "There is no rescheduling operation with triggerId=%s for job %s.",
                                        key.getTriggerId(), key.getJobId()),
                                HttpResponseStatus.NOT_FOUND));
            }
            return Optional.empty();
        }

        protected AsynchronousJobOperationKey getOperationKey(
                HandlerRequest<EmptyRequestBody> request) {
            final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            return AsynchronousJobOperationKey.of(triggerId, jobId);
        }
    }

    private static RestHandlerException createInternalServerError(
            Throwable throwable, AsynchronousJobOperationKey key, String errorMessageInfix) {
        return new RestHandlerException(
                String.format(
                        "Internal server error while %s rescheduling operation with triggerId=%s for job %s.",
                        errorMessageInfix, key.getTriggerId(), key.getJobId()),
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                throwable);
    }
}
