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

        @JsonCreator
        VertexMapping(@JsonProperty("vertexID") String vertexID, @JsonProperty("ssg") int ssg) {
            this.vertexID = vertexID;
            this.ssg = ssg;
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

        @Override
        public String toString() {
            return "VertexMapping{" + "vertexID='" + vertexID + '\'' + ", ssg=" + ssg + '}';
        }
    }

    public static class SSGMapping implements Serializable {
        private int index;
        private double cpu;
        private int managed;
        private int heap;
        private int network;
        private int offHeap;

        @JsonCreator
        SSGMapping(
                @JsonProperty("index") int index,
                @JsonProperty("cpu") double cpu,
                @JsonProperty("managed") int managed,
                @JsonProperty("heap") int heap,
                @JsonProperty("network") int network,
                @JsonProperty("offHeap") int offHeap) {
            this.index = index;
            this.cpu = cpu;
            this.managed = managed;
            this.heap = heap;
            this.network = network;
            this.offHeap = offHeap;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public double getCPU() {
            return cpu;
        }

        public void setCPU(double cpu) {
            this.cpu = cpu;
        }

        public int getManaged() {
            return managed;
        }

        public void setManaged(int managed) {
            this.managed = managed;
        }

        public int getHeap() {
            return heap;
        }

        public void setHeap(int heap) {
            this.heap = heap;
        }

        public int getNetwork() {
            return network;
        }

        public void setNetwork(int network) {
            this.network = network;
        }

        public int getOffHeap() {
            return offHeap;
        }

        public void setOffHeap(int offHeap) {
            this.offHeap = offHeap;
        }

        @Override
        public String toString() {
            return "SSGMapping{"
                    + "index="
                    + index
                    + ", cpu="
                    + cpu
                    + ", managed="
                    + managed
                    + ", heap="
                    + heap
                    + ", network="
                    + network
                    + ", offHeap="
                    + offHeap
                    + '}';
        }
    }
}
