package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** Mapper for the Json plan of rescheduling. */
public class ReschedulePlanJSONMapper implements Serializable {
    private static final long serialVersionUID = 1L;

    public ReschedulePlanJSONMapper() {}

    @JsonCreator
    public ReschedulePlanJSONMapper(
            @JsonProperty("vertexID") String vertexID,
            @JsonProperty("cpu") double cpu,
            @JsonProperty("managed") int managed,
            @JsonProperty("heap") int heap,
            @JsonProperty("network") int network,
            @JsonProperty("offHeap") int offHeap) {
        this.vertexID = vertexID;
        this.cpu = cpu;
        this.managed = managed;
        this.heap = heap;
        this.network = network;
        this.offHeap = offHeap;
    }

    private String vertexID;
    private double cpu;
    private int managed;
    private int heap;
    private int network;
    private int offHeap;

    public String getVertexID() {
        return vertexID;
    }

    public void setVertexID(String vertexID) {
        this.vertexID = vertexID;
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
        return "vertexID: "
                + vertexID
                + ",\n"
                + "cpu: "
                + cpu
                + ",\n"
                + "heap: "
                + heap
                + ",\n"
                + "managed: "
                + managed
                + ",\n"
                + "network: "
                + network
                + ",\n"
                + "offHeap: "
                + offHeap;
    }
}
