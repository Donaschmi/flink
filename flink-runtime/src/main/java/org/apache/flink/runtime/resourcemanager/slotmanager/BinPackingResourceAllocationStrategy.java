package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinPackingResourceAllocationStrategy implements ResourceAllocationStrategy{

    private final ResourceProfile totalResourceProfile;
    private final int numSlotsPerWorker;

    private int neededTaskManagers = -1;


    public BinPackingResourceAllocationStrategy(
            ResourceProfile totalResourceProfile,
            int numSlotsPerWorker) {
        this.totalResourceProfile = totalResourceProfile;
        this.numSlotsPerWorker = numSlotsPerWorker;
    }

    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {

        final ResourceAllocationResult.Builder resultBuilder = ResourceAllocationResult.builder();

        Collection<? extends TaskManagerInfo> registeredTaskManagers = taskManagerResourceInfoProvider.getRegisteredTaskManagers();
        Map<Integer, List<ResourceProfileJobID>> mapping = taskmanagerAllocations(missingResources);
        this.neededTaskManagers = mapping.size();
        //System.out.println(mapping);
        int index = 0;
        for (TaskManagerInfo tm:
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
                int targetTM = resourceRequirement.getTargetTM();
                if (targetTM == -1) {
                    return null;
                }
                if (!tmMap.containsKey(targetTM)) {
                    tmMap.put(targetTM, new ArrayList<ResourceProfileJobID>());
                }
                tmMap.get(targetTM).add(new ResourceProfileJobID(
                        resourceRequirements.getKey(),
                        resourceRequirement.getResourceProfile())
                );
            }
        }
        return tmMap;
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
