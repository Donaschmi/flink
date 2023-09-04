package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;

import java.util.Collection;

public class JustinSchedulingPlan extends JobSchedulingPlan {


    public JustinSchedulingPlan(
            VertexParallelism vertexParallelism,
            Collection<SlotAssignment> slotAssignments) {
        super(vertexParallelism, slotAssignments);
    }
}
