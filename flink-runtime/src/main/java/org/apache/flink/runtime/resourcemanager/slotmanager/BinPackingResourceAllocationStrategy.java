package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinPackingResourceAllocationStrategy implements ResourceAllocationStrategy {

    private final Logger LOG = LoggerFactory.getLogger(BinPackingResourceAllocationStrategy.class);

    private final ResourceProfile totalResourceProfile;
    private final int numSlotsPerWorker;
    private final boolean evenlySpreadOutSlots;

    private final Time taskManagerTimeout;

    /** Defines the number of redundant task managers. */
    private final int redundantTaskManagerNum;
    private int neededTaskManagers = -1;


    public BinPackingResourceAllocationStrategy(
            ResourceProfile totalResourceProfile,
            int numSlotsPerWorker,
            boolean evenlySpreadOutSlots,
            Time taskManagerTimeout,
            int redundantTaskManagerNum) {
        this.totalResourceProfile = totalResourceProfile;
        this.numSlotsPerWorker = numSlotsPerWorker;
        this.evenlySpreadOutSlots = evenlySpreadOutSlots
        ;
        this.taskManagerTimeout = taskManagerTimeout;
        this.redundantTaskManagerNum = redundantTaskManagerNum;
    }

    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {

        if (!allTargetTMDefined(missingResources)) {
            LOG.debug("Falling back to default strategy.");
            DefaultResourceAllocationStrategy strategy = new DefaultResourceAllocationStrategy(
                    totalResourceProfile,
                    numSlotsPerWorker,
                    evenlySpreadOutSlots,
                    taskManagerTimeout,
                    redundantTaskManagerNum
            );
            return strategy.tryFulfillRequirements(
                    missingResources,
                    taskManagerResourceInfoProvider,
                    blockedTaskManagerChecker
            );
        }

        final ResourceAllocationResult.Builder resultBuilder = ResourceAllocationResult.builder();

        Collection<? extends TaskManagerInfo> registeredTaskManagers = taskManagerResourceInfoProvider.getRegisteredTaskManagers();
        Map<Integer, List<ResourceProfileJobID>> mapping = taskmanagerAllocations(missingResources);
        LOG.debug("TaskManager allocations: " + mapping);
        this.neededTaskManagers = mapping.size();
        //System.out.println(mapping);
        int index = 0;
        for (TaskManagerInfo tm :
             registeredTaskManagers) {
            if (!mapping.containsKey(index))
                break;
            for (ResourceProfileJobID rp :
                    mapping.get(index)) {
                resultBuilder.addAllocationOnRegisteredResource(
                        rp.jobID,
                        tm.getInstanceId(),
                        rp.resourceProfile
                );
            }
            index++;
        }
        for (int i = registeredTaskManagers.size(); i < this.neededTaskManagers; i++) {
            // Add new pending resources
            final PendingTaskManager newPendingTaskManager = new PendingTaskManager(totalResourceProfile, numSlotsPerWorker);
            resultBuilder.addPendingTaskManagerAllocate(newPendingTaskManager);
            for (ResourceProfileJobID rp :
                    mapping.get(i)) {
                resultBuilder.addAllocationOnPendingResource(
                        rp.jobID,
                        newPendingTaskManager.getPendingTaskManagerId(),
                        rp.resourceProfile
                );
            }
        }
        return resultBuilder.build();
    }

    @Override
    public ResourceReconcileResult tryReconcileClusterResources(TaskManagerResourceInfoProvider taskManagerResourceInfoProvider) {
        ResourceReconcileResult.Builder builder = ResourceReconcileResult.builder();
        // We only need to specify how much task managers can be released.
        int index = 1;
        for (TaskManagerInfo tm :
                taskManagerResourceInfoProvider.getRegisteredTaskManagers()) {
            if (index > this.neededTaskManagers) {
                builder.addTaskManagerToRelease(tm);
            }
            index++;
        }
        return builder.build();
    }

    private static Map<Integer, List<ResourceProfileJobID>> taskmanagerAllocations(Map<JobID, Collection<ResourceRequirement>> missingResources) {
        Map<Integer, List<ResourceProfileJobID>> tmMap = new HashMap<>();
        for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements: missingResources.entrySet()) {
            for (ResourceRequirement resourceRequirement: resourceRequirements.getValue()) {
                int targetTM = resourceRequirement.getResourceProfile().getTargetTM();
                if (targetTM == -1) {
                    // Redirect to DefaultResourceAllocationStrategy
                    return new HashMap<>();
                }
                if (!tmMap.containsKey(targetTM)) {
                    tmMap.put(targetTM, new ArrayList<ResourceProfileJobID>());
                }
                for (int i = 0; i < resourceRequirement.getNumberOfRequiredSlots(); i++) {
                    tmMap.get(targetTM).add(new ResourceProfileJobID(
                            resourceRequirements.getKey(),
                            resourceRequirement.getResourceProfile())
                    );
                }
            }
        }
        return tmMap;
    }

    private boolean allTargetTMDefined(Map<JobID, Collection<ResourceRequirement>> missingResources) {
        for (Collection<ResourceRequirement> rqs :
                missingResources.values()) {
            for (ResourceRequirement rq :
                    rqs) {
                if (rq.getResourceProfile().getTargetTM() == -1) return false;
            }

        }
        return true;
    }

    private static class ResourceProfileJobID {
        private final JobID jobID;

        private final ResourceProfile resourceProfile;

        private ResourceProfileJobID(JobID jobID, ResourceProfile resourceProfile) {
            this.jobID = jobID;
            this.resourceProfile = resourceProfile;
        }
    }
}
