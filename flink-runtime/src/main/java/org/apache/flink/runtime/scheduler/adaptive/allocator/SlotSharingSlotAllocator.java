/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link SlotAllocator} implementation that supports slot sharing. */
public class SlotSharingSlotAllocator implements SlotAllocator {

    private final ReserveSlotFunction reserveSlotFunction;
    private final FreeSlotFunction freeSlotFunction;
    private final IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction;

    private SlotSharingSlotAllocator(
            ReserveSlotFunction reserveSlot,
            FreeSlotFunction freeSlotFunction,
            IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction) {
        this.reserveSlotFunction = reserveSlot;
        this.freeSlotFunction = freeSlotFunction;
        this.isSlotAvailableAndFreeFunction = isSlotAvailableAndFreeFunction;
    }

    public static SlotSharingSlotAllocator createSlotSharingSlotAllocator(
            ReserveSlotFunction reserveSlot,
            FreeSlotFunction freeSlotFunction,
            IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction) {
        return new SlotSharingSlotAllocator(
                reserveSlot, freeSlotFunction, isSlotAvailableAndFreeFunction);
    }

    @Override
    public ResourceCounter calculateRequiredSlots(
            Iterable<JobInformation.VertexInformation> vertices) {
        ResourceCounter resourceCounter = ResourceCounter.empty();
        for (Map.Entry<SlotSharingGroupId, ImmutablePair<ResourceProfile, Integer>>
                resourceProfile : getMaxParallelismForSlotSharingGroups(vertices).entrySet()) {
            resourceCounter =
                    resourceCounter.add(
                            resourceProfile.getValue().getKey(),
                            resourceProfile.getValue().getValue());
            LoggerFactory.getLogger(SlotSharingSlotAllocator.class)
                    .debug("resourceCounter: " + resourceCounter);
        }
        return resourceCounter;
    }

    private static Map<SlotSharingGroupId, ImmutablePair<ResourceProfile, Integer>>
            getMaxParallelismForSlotSharingGroups(
                    Iterable<JobInformation.VertexInformation> vertices) {
        final Map<SlotSharingGroupId, ImmutablePair<ResourceProfile, Integer>>
                maxParallelismForSlotSharingGroups = new HashMap<>();
        for (JobInformation.VertexInformation vertex : vertices) {
            maxParallelismForSlotSharingGroups.compute(
                    vertex.getSlotSharingGroup().getSlotSharingGroupId(),
                    (slotSharingGroupId, pair) ->
                            pair == null
                                    ? ImmutablePair.of(
                                            vertex.getSlotSharingGroup().getResourceProfile(),
                                            vertex.getParallelism())
                                    : ImmutablePair.of(
                                            vertex.getSlotSharingGroup().getResourceProfile(),
                                            Math.max(pair.getRight(), vertex.getParallelism())));
        }
        return maxParallelismForSlotSharingGroups;
    }

    @Override
    public Optional<VertexParallelismWithSlotSharing> determineParallelism(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            Collection<ResourceProfile> totalResources) {
        // TODO: This can waste slots if the max parallelism for slot sharing groups is not equal

        final int slotsPerSlotSharingGroup =
                freeSlots.size() / jobInformation.getSlotSharingGroups().size();

        if (slotsPerSlotSharingGroup == 0) {
            // => less slots than slot-sharing groups
            return Optional.empty();
        }

        boolean isUnknown =
                jobInformation.getSlotSharingGroups().stream()
                                .flatMap(
                                        slotSharingGroup ->
                                                slotSharingGroup.getJobVertexIds().stream())
                                .map(jobInformation::getVertexInformation)
                                .anyMatch(
                                        vertexInformation ->
                                                ResourceProfile.UNKNOWN.equals(
                                                        vertexInformation
                                                                .getSlotSharingGroup()
                                                                .getResourceProfile()))
                        && (totalResources != null);
        int adaptedParallelism = 0;
        if (!isUnknown) {
            adaptedParallelism = getAdjustedParallelism(jobInformation, totalResources);
        }

        final Iterator<? extends SlotInfo> slotIterator = freeSlots.iterator();
        List<AllocationID> allocatedSlots = new ArrayList<>();
        final Collection<ExecutionSlotSharingGroupAndSlot> assignments = new ArrayList<>();
        final Map<JobVertexID, Integer> allVertexParallelism = new HashMap<>();

        freeSlots.forEach(
                slot ->
                        LoggerFactory.getLogger(SlotSharingSlotAllocator.class)
                                .debug("freeslots: " + slot.getAllocationId()));

        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            final List<JobInformation.VertexInformation> containedJobVertices =
                    slotSharingGroup.getJobVertexIds().stream()
                            .map(jobInformation::getVertexInformation)
                            .collect(Collectors.toList());

            final Map<JobVertexID, Integer> vertexParallelism =
                    determineParallelism(
                            containedJobVertices,
                            isUnknown ? slotsPerSlotSharingGroup : adaptedParallelism);

            vertexParallelism.forEach(
                    (k, v) ->
                            LoggerFactory.getLogger(SlotSharingSlotAllocator.class)
                                    .debug(
                                            "Debug mapping: "
                                                    + k.toHexString()
                                                    + ":"
                                                    + v.toString()));
            final Iterable<ExecutionSlotSharingGroup> sharedSlotToVertexAssignment =
                    createExecutionSlotSharingGroups(vertexParallelism);

            for (ExecutionSlotSharingGroup executionSlotSharingGroup :
                    sharedSlotToVertexAssignment) {
                SlotInfo slotInfo =
                        freeSlots.stream()
                                .filter(
                                        slot ->
                                                slot.getResourceProfile()
                                                        .equals(
                                                                slotSharingGroup
                                                                        .getResourceProfile()))
                                .filter(slot -> !allocatedSlots.contains(slot.getAllocationId()))
                                .findFirst()
                                .orElse(null);
                if (slotInfo == null) {
                    LoggerFactory.getLogger(SlotSharingSlotAllocator.class).debug("No slot");
                    slotInfo = slotIterator.next();
                } else {
                    LoggerFactory.getLogger(SlotSharingSlotAllocator.class)
                            .debug("Adding new slot : " + slotInfo.getAllocationId());
                    allocatedSlots.add(slotInfo.getAllocationId());
                }
                allocatedSlots.forEach(
                        slot ->
                                LoggerFactory.getLogger(SlotSharingSlotAllocator.class)
                                        .debug("debugg: " + slot));

                assignments.add(
                        new ExecutionSlotSharingGroupAndSlot(executionSlotSharingGroup, slotInfo));
            }
            allVertexParallelism.putAll(vertexParallelism);
        }

        return Optional.of(new VertexParallelismWithSlotSharing(allVertexParallelism, assignments));
    }

    @VisibleForTesting
    public static int getMaxParallelism(JobInformation jobInformation) {
        int max = 0;
        for (SlotSharingGroup slot : jobInformation.getSlotSharingGroups()) {
            max =
                    Math.max(
                            max,
                            slot.getJobVertexIds().stream()
                                    .map(jobInformation::getVertexInformation)
                                    .map(JobInformation.VertexInformation::getParallelism)
                                    .max(Comparator.naturalOrder())
                                    .get());
        }
        return max;
    }

    private static Map<JobVertexID, Integer> determineParallelism(
            Collection<JobInformation.VertexInformation> containedJobVertices, int availableSlots) {
        final Map<JobVertexID, Integer> vertexParallelism = new HashMap<>();
        for (JobInformation.VertexInformation jobVertex : containedJobVertices) {
            final int parallelism = Math.min(jobVertex.getParallelism(), availableSlots);

            vertexParallelism.put(jobVertex.getJobVertexID(), parallelism);
        }

        return vertexParallelism;
    }

    public static int getAdjustedParallelism(
            JobInformation jobInformation, Collection<ResourceProfile> totalResources) {
        // Get the absolute max parallelism among all operators
        int currentParallelism = getMaxParallelism(jobInformation);
        ResourceCounter requiredResources = ResourceCounter.empty();

        while (currentParallelism != 0) {
            LoggerFactory.getLogger(SlotSharingSlotAllocator.class)
                    .debug("currentParallelism: " + currentParallelism);
            for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
                int finalCurrentParallelism = currentParallelism;
                final int parallelism =
                        slotSharingGroup.getJobVertexIds().stream()
                                .map(jobInformation::getVertexInformation)
                                .map(
                                        vertexInformation ->
                                                Math.min(
                                                        vertexInformation.getParallelism(),
                                                        finalCurrentParallelism))
                                .max(Comparator.naturalOrder())
                                .get();
                requiredResources =
                        requiredResources.add(slotSharingGroup.getResourceProfile(), parallelism);
            }

            LoggerFactory.getLogger(SlotSharingSlotAllocator.class)
                    .debug("Matching configuration: " + requiredResources);
            if (AllocatorUtils.canNewRequirementBeFulfilled(
                    (List<ResourceProfile>) totalResources, requiredResources)) {
                LoggerFactory.getLogger(SlotSharingSlotAllocator.class)
                        .debug(
                                "found working allocation for parallelism: "
                                        + currentParallelism
                                        + " , "
                                        + currentParallelism);
                return currentParallelism;
            }
            currentParallelism--;
            requiredResources = ResourceCounter.empty();
        }
        return 0;
    }

    private static Iterable<ExecutionSlotSharingGroup> createExecutionSlotSharingGroups(
            Map<JobVertexID, Integer> containedJobVertices) {
        final Map<Integer, Set<ExecutionVertexID>> sharedSlotToVertexAssignment = new HashMap<>();

        for (Map.Entry<JobVertexID, Integer> jobVertex : containedJobVertices.entrySet()) {
            for (int i = 0; i < jobVertex.getValue(); i++) {
                sharedSlotToVertexAssignment
                        .computeIfAbsent(i, ignored -> new HashSet<>())
                        .add(new ExecutionVertexID(jobVertex.getKey(), i));
            }
        }

        return sharedSlotToVertexAssignment.values().stream()
                .map(ExecutionSlotSharingGroup::new)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<ReservedSlots> tryReserveResources(VertexParallelism vertexParallelism) {
        Preconditions.checkArgument(
                vertexParallelism instanceof VertexParallelismWithSlotSharing,
                String.format(
                        "%s expects %s as argument.",
                        SlotSharingSlotAllocator.class.getSimpleName(),
                        VertexParallelismWithSlotSharing.class.getSimpleName()));

        final VertexParallelismWithSlotSharing vertexParallelismWithSlotSharing =
                (VertexParallelismWithSlotSharing) vertexParallelism;

        final Collection<AllocationID> expectedSlots =
                calculateExpectedSlots(vertexParallelismWithSlotSharing.getAssignments());

        if (areAllExpectedSlotsAvailableAndFree(expectedSlots)) {
            final Map<ExecutionVertexID, LogicalSlot> assignedSlots = new HashMap<>();

            for (ExecutionSlotSharingGroupAndSlot executionSlotSharingGroup :
                    vertexParallelismWithSlotSharing.getAssignments()) {
                final SharedSlot sharedSlot =
                        reserveSharedSlot(executionSlotSharingGroup.getSlotInfo());

                for (ExecutionVertexID executionVertexId :
                        executionSlotSharingGroup
                                .getExecutionSlotSharingGroup()
                                .getContainedExecutionVertices()) {
                    final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();
                    assignedSlots.put(executionVertexId, logicalSlot);
                }
            }

            return Optional.of(ReservedSlots.create(assignedSlots));
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    private Collection<AllocationID> calculateExpectedSlots(
            Iterable<? extends ExecutionSlotSharingGroupAndSlot> assignments) {
        final Collection<AllocationID> requiredSlots = new ArrayList<>();

        for (ExecutionSlotSharingGroupAndSlot assignment : assignments) {
            requiredSlots.add(assignment.getSlotInfo().getAllocationId());
        }
        return requiredSlots;
    }

    private boolean areAllExpectedSlotsAvailableAndFree(
            Iterable<? extends AllocationID> requiredSlots) {
        for (AllocationID requiredSlot : requiredSlots) {
            if (!isSlotAvailableAndFreeFunction.isSlotAvailableAndFree(requiredSlot)) {
                return false;
            }
        }

        return true;
    }

    private SharedSlot reserveSharedSlot(SlotInfo slotInfo) {
        final PhysicalSlot physicalSlot =
                reserveSlotFunction.reserveSlot(
                        slotInfo.getAllocationId(), slotInfo.getResourceProfile());

        return new SharedSlot(
                new SlotRequestId(),
                physicalSlot,
                slotInfo.willBeOccupiedIndefinitely(),
                () ->
                        freeSlotFunction.freeSlot(
                                slotInfo.getAllocationId(), null, System.currentTimeMillis()));
    }

    static class ExecutionSlotSharingGroup {
        private final Set<ExecutionVertexID> containedExecutionVertices;

        public ExecutionSlotSharingGroup(Set<ExecutionVertexID> containedExecutionVertices) {
            this.containedExecutionVertices = containedExecutionVertices;
        }

        public Collection<ExecutionVertexID> getContainedExecutionVertices() {
            return containedExecutionVertices;
        }
    }

    static class ExecutionSlotSharingGroupAndSlot {
        private final ExecutionSlotSharingGroup executionSlotSharingGroup;
        private final SlotInfo slotInfo;

        public ExecutionSlotSharingGroupAndSlot(
                ExecutionSlotSharingGroup executionSlotSharingGroup, SlotInfo slotInfo) {
            this.executionSlotSharingGroup = executionSlotSharingGroup;
            this.slotInfo = slotInfo;
        }

        public ExecutionSlotSharingGroup getExecutionSlotSharingGroup() {
            return executionSlotSharingGroup;
        }

        public SlotInfo getSlotInfo() {
            return slotInfo;
        }
    }
}
