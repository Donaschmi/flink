package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Arrays;

/** Mapper for the Json plan of rescheduling. */
public class ReschedulePlanJSONMapper implements Serializable {
    private static final long serialVersionUID = 1L;

    private VertexMapping[] vertexMapping;
    private SSGMapping[] ssgMapping;

    public ReschedulePlanJSONMapper() {}

    @JsonCreator
    public ReschedulePlanJSONMapper(
            @JsonProperty("vertexMapping") VertexMapping[] vertexMapping,
            @JsonProperty("ssgMapping") SSGMapping[] ssgMapping) {
        this.vertexMapping = vertexMapping;
        this.ssgMapping = ssgMapping;
    }

    public SSGMapping[] getSsgMapping() {
        return ssgMapping;
    }

    public void setSsgMapping(SSGMapping[] ssgMapping) {
        this.ssgMapping = ssgMapping;
    }

    public VertexMapping[] getVertexMapping() {
        return vertexMapping;
    }

    public void setVertexMapping(VertexMapping[] vertexMapping) {
        this.vertexMapping = vertexMapping;
    }

    @Override
    public String toString() {
        return "ReschedulePlanJSONMapper{"
                + "vertexMapping="
                + Arrays.toString(vertexMapping)
                + ", ssgMapping="
                + Arrays.toString(ssgMapping)
                + '}';
    }

    public static class VertexMapping implements Serializable {
        private String vertexID;
        private int ssg;
        private int parallelism;

        @JsonCreator
        VertexMapping(@JsonProperty("vertexID") String vertexID, @JsonProperty("ssg") int ssg, @JsonProperty("parallelism") int parallelism) {
            this.vertexID = vertexID;
            this.ssg = ssg;
            this.parallelism = parallelism;
        }

        public String getVertexID() {
            return vertexID;
        }

        public void setVertexID(String vertexID) {
            this.vertexID = vertexID;
        }

        public int getSsg() {
            return ssg;
        }

        public void setSsg(int ssg) {
            this.ssg = ssg;
        }

        public int getParallelism() {
            return parallelism;
        }

        public void setParallelism(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public String toString() {
            return "VertexMapping{" +
                    "vertexID='" + vertexID + '\'' +
                    ", ssg=" + ssg +
                    ", parallelism=" + parallelism +
                    '}';
        }
    }

    public static class SSGMapping implements Serializable {
        private int index;
        private double cpuCores;
        private int managedMemory;
        private int taskHeapMemory;
        private int networkMemory;
        private int taskOffHeapMemory;

        @JsonCreator
        SSGMapping(
                @JsonProperty("index") int index,
                @JsonProperty("cpuCores") double cpuCores,
                @JsonProperty("managedMemory") int managedMemory,
                @JsonProperty("taskHeapMemory") int taskHeapMemory,
                @JsonProperty("networkMemory") int networkMemory,
                @JsonProperty("taskOffHeapMemory") int taskOffHeapMemory) {
            this.index = index;
            this.cpuCores = cpuCores;
            this.managedMemory = managedMemory;
            this.taskHeapMemory = taskHeapMemory;
            this.networkMemory = networkMemory;
            this.taskOffHeapMemory = taskOffHeapMemory;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public double getCpuCores() {
            return cpuCores;
        }

        public void setCpuCores(double cpuCores) {
            this.cpuCores = cpuCores;
        }

        public int getManagedMemory() {
            return managedMemory;
        }

        public void setManagedMemory(int managedMemory) {
            this.managedMemory = managedMemory;
        }

        public int getTaskHeapMemory() {
            return taskHeapMemory;
        }

        public void setTaskHeapMemory(int taskHeapMemory) {
            this.taskHeapMemory = taskHeapMemory;
        }

        public int getNetworkMemory() {
            return networkMemory;
        }

        public void setNetworkMemory(int networkMemory) {
            this.networkMemory = networkMemory;
        }

        public int getTaskOffHeapMemory() {
            return taskOffHeapMemory;
        }

        public void setTaskOffHeapMemory(int taskOffHeapMemory) {
            this.taskOffHeapMemory = taskOffHeapMemory;
        }

        @Override
        public String toString() {
            return "SSGMapping{"
                    + "index="
                    + index
                    + ", cpu="
                    + cpuCores
                    + ", managed="
                    + managedMemory
                    + ", heap="
                    + taskHeapMemory
                    + ", network="
                    + networkMemory
                    + ", offHeap="
                    + taskOffHeapMemory
                    + '}';
        }
    }
}
