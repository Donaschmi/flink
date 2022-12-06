package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;

import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class RequestTotalResourcesFunction {

    private ResourceManagerGateway resourceManagerGateway;
    private final Time timeout;

    private boolean isUp = false;

    public RequestTotalResourcesFunction(Time timeout) {
        this.timeout = timeout;
    }

    public void setResourceManagerGateway(ResourceManagerGateway resourceManagerGateway) {
        this.resourceManagerGateway = resourceManagerGateway;
        this.isUp = true;
    }

    public boolean isGatewayAvailable() {
        return this.isUp;
    }

    public Collection<ResourceProfile> requestTotalResources() {
        assert (resourceManagerGateway != null);
        try {
            LoggerFactory.getLogger(RequestTotalResourcesFunction.class)
                    .debug("resourceManagerGateway: " + resourceManagerGateway);
            CompletableFuture<Collection<TaskManagerInfo>> collectionCompletableFuture =
                    resourceManagerGateway.requestTaskManagerInfo(timeout);
            return collectionCompletableFuture.get().stream()
                    .map(TaskManagerInfo::getTotalResourceProfile)
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
