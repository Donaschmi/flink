package org.apache.flink.runtime.jobgraph.charon;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.justin.JustinVertexResourceRequirements;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;


public class CharonVertexResourceRequirements  extends JustinVertexResourceRequirements {

    private static final String FIELD_NAME_PARALLELISM = "parallelism";
    private static final String FIELD_NAME_RESOURCE_PROFILE = "resourceProfile";
    private static final String FIELD_NAME_TARGET_TM = "targetTM";

    private final int targetTM;

    public CharonVertexResourceRequirements(
            @JsonProperty(FIELD_NAME_PARALLELISM) Parallelism parallelism,
            @JsonProperty(FIELD_NAME_RESOURCE_PROFILE) ResourceProfile resourceProfile,
            @JsonProperty(FIELD_NAME_TARGET_TM) int targetTM) {
        super(parallelism, resourceProfile);
        this.targetTM = targetTM;
    }

    public int getTargetTM() {
        return targetTM;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CharonVertexResourceRequirements that = (CharonVertexResourceRequirements) o;
        return getParallelism().equals(that.getParallelism())
                && getResourceProfile().isMatching(that.getResourceProfile())
                && getTargetTM() == that.getTargetTM();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getParallelism(), getResourceProfile(), getTargetTM());
    }

    @Override
    public String toString() {
        return "CharonVertexResourceRequirements{"
                + "parallelism="
                + getParallelism()
                + ", resourceProfile="
                + getResourceProfile()
                + ", targetTM=" + targetTM +
                + '}';
    }
}
