/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.CompletedScheduledFuture;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.ReschedulePlanJSONMapper;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link Rescheduling} state of the {@link AdaptiveScheduler}. */
public class ReschedulingTest extends TestLogger {

    @Test
    public void testExecutionGraphCancellationOnEnter() throws Exception {
        try (MockReschedulingContext ctx = new MockReschedulingContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            createReschedulingState(ctx, mockExecutionGraph);

            assertThat(mockExecutionGraph.getState(), is(JobStatus.CANCELLING));
        }
    }

    @Test
    public void testTransitionToWaitingForResourcesWhenCancellationComplete() throws Exception {
        try (MockReschedulingContext ctx = new MockReschedulingContext()) {
            Rescheduling rescheduling = createReschedulingState(ctx);
            ctx.setExpectWaitingForResources();
            rescheduling.onGloballyTerminalState(JobStatus.CANCELED);
        }
    }

    @Test
    public void testCancel() throws Exception {
        try (MockReschedulingContext ctx = new MockReschedulingContext()) {
            Rescheduling rescheduling = createReschedulingState(ctx);
            ctx.setExpectCancelling(assertNonNull());
            rescheduling.cancel();
        }
    }

    @Test
    public void testSuspend() throws Exception {
        try (MockReschedulingContext ctx = new MockReschedulingContext()) {
            Rescheduling rescheduling = createReschedulingState(ctx);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED)));
            final Throwable cause = new RuntimeException("suspend");
            rescheduling.suspend(cause);
        }
    }

    @Test
    public void testGlobalFailuresAreIgnored() throws Exception {
        try (MockReschedulingContext ctx = new MockReschedulingContext()) {
            Rescheduling rescheduling = createReschedulingState(ctx);
            rescheduling.handleGlobalFailure(new RuntimeException());
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testStateDoesNotExposeGloballyTerminalExecutionGraph() throws Exception {
        try (MockReschedulingContext ctx = new MockReschedulingContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            Rescheduling rescheduling = createReschedulingState(ctx, mockExecutionGraph);

            // ideally we'd just delay the state transitions, but the context does not support that
            ctx.setExpectWaitingForResources();
            mockExecutionGraph.completeTerminationFuture(JobStatus.CANCELED);

            // this is just a sanity check for the test
            assertThat(rescheduling.getExecutionGraph().getState(), is(JobStatus.CANCELED));

            assertThat(rescheduling.getJobStatus(), is(JobStatus.RESCHEDULING));
            assertThat(rescheduling.getJob().getState(), is(JobStatus.RESCHEDULING));
            assertThat(rescheduling.getJob().getStatusTimestamp(JobStatus.CANCELED), is(0L));
        }
    }

    @Test
    public void testJSONWellConvertedToMap() throws JsonProcessingException {
        String json =
                "{ \n"
                        + "  \"vertices\": [\n"
                        + "    {\n"
                        + "      \"vertexID\": \"ebca99d2ba186d39f4b704d5595984ad\",\n"
                        + "      \"ssg\": 1\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"vertexID\": \"365ccfea623eaebf17f36c5a0cdc4ddc\",\n"
                        + "      \"ssg\": 2\n"
                        + "    }\n"
                        + "  ],\n"
                        + "  \"ssg\":\n"
                        + "  [\n"
                        + "    {\n"
                        + "      \"index\": 1,\n"
                        + "      \"cpu\": 0.6,\n"
                        + "      \"managed\": 1000,\n"
                        + "      \"heap\": 10,\n"
                        + "      \"network\": 10,\n"
                        + "      \"offHeap\": 0\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"index\": 2,\n"
                        + "      \"cpu\": 0.4,\n"
                        + "      \"managed\": 60,\n"
                        + "      \"heap\": 100,\n"
                        + "      \"network\": 10,\n"
                        + "      \"offHeap\": 0\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        ObjectMapper mapper = new ObjectMapper();
        ReschedulePlanJSONMapper mapping = mapper.readValue(json, ReschedulePlanJSONMapper.class);
        Map<JobVertexID, SlotSharingGroup> map = AdaptiveScheduler.convertJSONMapperToMap(mapping);
        assertThat(
                map.containsKey(JobVertexID.fromHexString("ebca99d2ba186d39f4b704d5595984ad")),
                is(true));
        assertThat(
                map.containsKey(JobVertexID.fromHexString("ebca99d2ba186d39f4b704d5595984ad")),
                is(true));
        assertThat(
                map.get(JobVertexID.fromHexString("90bea66de1c231edf33913ecd54406c1"))
                        .getResourceProfile()
                        .equals(
                                ResourceProfile.newBuilder()
                                        .setCpuCores(0.6)
                                        .setTaskHeapMemoryMB(10)
                                        .setManagedMemoryMB(100)
                                        .setNetworkMemoryMB(10)
                                        .setTaskOffHeapMemoryMB(0)
                                        .build()),
                is(true));
        assertThat(
                map.get(JobVertexID.fromHexString("365ccfea623eaebf17f36c5a0cdc4ddc"))
                        .getResourceProfile()
                        .equals(
                                ResourceProfile.newBuilder()
                                        .setCpuCores(0.4)
                                        .setTaskHeapMemoryMB(100)
                                        .setManagedMemoryMB(60)
                                        .setNetworkMemoryMB(10)
                                        .setTaskOffHeapMemoryMB(0)
                                        .build()),
                is(true));
    }

    public Rescheduling createReschedulingState(
            MockReschedulingContext ctx, ExecutionGraph executionGraph) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new TestingOperatorCoordinatorHandler();
        executionGraph.transitionToRunning();
        return new Rescheduling(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                null,
                log,
                Duration.ZERO,
                ClassLoader.getSystemClassLoader(),
                new ArrayList<>());
    }

    public Rescheduling createReschedulingState(MockReschedulingContext ctx)
            throws JobException, JobExecutionException {
        return createReschedulingState(ctx, new StateTrackingMockExecutionGraph());
    }

    private static class MockReschedulingContext extends MockStateWithExecutionGraphContext
            implements Rescheduling.Context {

        private final StateValidator<ExecutingTest.CancellingArguments> cancellingStateValidator =
                new StateValidator<>("Cancelling");

        private final StateValidator<Void> waitingForResourcesStateValidator =
                new StateValidator<>("WaitingForResources");

        public void setExpectCancelling(Consumer<ExecutingTest.CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        public void setExpectWaitingForResources() {
            waitingForResourcesStateValidator.expectInput((none) -> {});
        }

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                List<ExceptionHistoryEntry> failureCollection) {
            cancellingStateValidator.validateInput(
                    new ExecutingTest.CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
            hadStateTransition = true;
        }

        @Override
        public void archiveFailure(RootExceptionHistoryEntry failure) {}

        @Override
        public void goToWaitingForResources() {
            waitingForResourcesStateValidator.validateInput(null);
            hadStateTransition = true;
        }

        @Override
        public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
            if (!hadStateTransition) {
                action.run();
            }
            return CompletedScheduledFuture.create(null);
        }

        @Override
        public void close() throws Exception {
            super.close();
            cancellingStateValidator.close();
            waitingForResourcesStateValidator.close();
        }
    }
}
