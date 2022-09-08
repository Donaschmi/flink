package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

/** State which represents a rescheduling job with an {@link ExecutionGraph} and assigned slots. */
public class Rescheduling extends StateWithExecutionGraph {

    private final Context context;

    private final Duration backoffTime;

    @Nullable private ScheduledFuture<?> goToWaitingForResourcesFuture;
    private final Map<JobVertexID, SlotSharingGroup> reschedulingPlan;

    Rescheduling(
            Context context,
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Map<JobVertexID, SlotSharingGroup> reschedulingPlan,
            Logger logger,
            Duration backoffTime,
            ClassLoader userCodeClassLoader,
            List<ExceptionHistoryEntry> failureCollection) {
        super(
                context,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                logger,
                userCodeClassLoader,
                failureCollection);
        this.context = context;
        this.backoffTime = backoffTime;
        this.reschedulingPlan = reschedulingPlan;

        getExecutionGraph().cancel();
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        if (goToWaitingForResourcesFuture != null) {
            goToWaitingForResourcesFuture.cancel(false);
        }

        super.onLeave(newState);
    }

    @Override
    public void cancel() {
        context.goToCanceling(
                getExecutionGraph(),
                getExecutionGraphHandler(),
                getOperatorCoordinatorHandler(),
                getFailures());
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.RESCHEDULING;
    }

    @Override
    void onFailure(Throwable cause) {}

    @Override
    void onGloballyTerminalState(JobStatus globallyTerminalState) {
        Preconditions.checkArgument(globallyTerminalState == JobStatus.CANCELED);
        try {
            Thread.sleep(10000);

        } catch (Exception ignored) {

        }

        goToWaitingForResourcesFuture =
                context.runIfState(this, context::goToWaitingForResources, backoffTime);
    }

    /** Context of the {@link Restarting} state. */
    interface Context
            extends StateWithExecutionGraph.Context,
                    StateTransitions.ToCancelling,
                    StateTransitions.ToWaitingForResources {

        /**
         * Runs the given action after the specified delay if the state is the expected state at
         * this time.
         *
         * @param expectedState expectedState describes the required state to run the action after
         *     the delay
         * @param action action to run if the state equals the expected state
         * @param delay delay after which the action should be executed
         * @return a ScheduledFuture representing pending completion of the task
         */
        ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay);
    }

    static class Factory implements StateFactory<Rescheduling> {

        private final Rescheduling.Context context;
        private final Logger log;
        private final ExecutionGraph executionGraph;
        private final ExecutionGraphHandler executionGraphHandler;
        private final OperatorCoordinatorHandler operatorCoordinatorHandler;
        private final Map<JobVertexID, SlotSharingGroup> reschedulingPlan;
        private final Duration backoffTime;
        private final ClassLoader userCodeClassLoader;
        private final List<ExceptionHistoryEntry> failureCollection;

        public Factory(
                Rescheduling.Context context,
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Map<JobVertexID, SlotSharingGroup> reschedulingPlan,
                Logger log,
                Duration backoffTime,
                ClassLoader userCodeClassLoader,
                List<ExceptionHistoryEntry> failureCollection) {
            this.context = context;
            this.log = log;
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.reschedulingPlan = reschedulingPlan;
            this.backoffTime = backoffTime;
            this.userCodeClassLoader = userCodeClassLoader;
            this.failureCollection = failureCollection;
        }

        public Class<Rescheduling> getStateClass() {
            return Rescheduling.class;
        }

        public Rescheduling getState() {
            return new Rescheduling(
                    context,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    reschedulingPlan,
                    log,
                    backoffTime,
                    userCodeClassLoader,
                    failureCollection);
        }
    }
}
