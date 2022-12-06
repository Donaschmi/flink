package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AllocatorUtils {

    public static boolean canNewRequirementBeFulfilled(
            List<ResourceProfile> availableResources, ResourceCounter requiredResources) {
        List<ResourceProfile> resources = new ArrayList<>();
        for (Map.Entry<ResourceProfile, Integer> r : requiredResources.getResourcesWithCount()) {
            for (int i = 0; i < r.getValue(); i++) {
                resources.add(r.getKey());
            }
        }
        return canNewRequirementBeFulfilled(availableResources, resources);
    }

    public static boolean canNewRequirementBeFulfilled(
            List<ResourceProfile> availableResources, List<ResourceProfile> requiredResources) {
        if (!checkOutStandingRequirement(availableResources, requiredResources)) {
            return false;
        }
        return tryFitRequirement(availableResources, requiredResources)
                <= availableResources.size();
    }

    private static boolean checkOutStandingRequirement(
            List<ResourceProfile> availableResources, List<ResourceProfile> requiredResources) {
        ResourceProfile requirementSummed = ResourceProfile.ZERO;
        for (ResourceProfile resourceProfile : requiredResources) {
            requirementSummed = requirementSummed.merge(resourceProfile);
        }
        ResourceProfile availableSummed = ResourceProfile.ZERO;
        for (ResourceProfile resourceProfile : availableResources) {
            availableSummed = availableSummed.merge(resourceProfile);
        }
        return availableSummed.allFieldsNoLessThan(requirementSummed);
    }

    private static int best;

    private static int tryFitRequirement(
            List<ResourceProfile> availableResources, List<ResourceProfile> requiredResources) {
        best = requiredResources.size();
        place(
                new ResourceProfile[best],
                0,
                requiredResources.toArray(new ResourceProfile[] {}),
                0,
                availableResources.get(0));
        return best;
    }

    private static void place(
            ResourceProfile[] bins,
            int used,
            ResourceProfile[] resourceProfiles,
            int index,
            ResourceProfile capacity) {
        if (used >= best) {
            return;
        }
        if (index >= bins.length) {
            best = used;
            return;
        }

        Set<ResourceProfile> set = new HashSet<>();

        for (int i = 0; i < used; i++) {
            if (!set.add(bins[i])) {
                continue;
            }
            System.out.println(bins[i].merge(resourceProfiles[index]));
            if (capacity.allFieldsNoLessThan(bins[i].merge(resourceProfiles[index]))) {
                bins[i] = bins[i].merge(resourceProfiles[index]);
                place(bins, used, resourceProfiles, index + 1, capacity);
                bins[i] = bins[i].subtract(resourceProfiles[index]);
            }
        }
        bins[used] = resourceProfiles[index];
        place(bins, used + 1, resourceProfiles, index + 1, capacity);
        bins[used] = ResourceProfile.ZERO;
    }
}
