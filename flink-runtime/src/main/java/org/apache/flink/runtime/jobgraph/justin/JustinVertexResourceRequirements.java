package org.apache.flink.runtime.jobgraph.justin;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class JustinVertexResourceRequirements {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_NAME_PARALLELISM = "parallelism";
    private static final String FIELD_NAME_RESOURCE_PROFILE = "resourceProfile";

    public static class Parallelism implements Serializable {

        private static final String FIELD_NAME_LOWER_BOUND = "lowerBound";
        private static final String FIELD_NAME_UPPER_BOUND = "upperBound";

        @JsonProperty(FIELD_NAME_LOWER_BOUND)
        private final int lowerBound;

        @JsonProperty(FIELD_NAME_UPPER_BOUND)
        private final int upperBound;

        @JsonCreator
        public Parallelism(
                @JsonProperty(FIELD_NAME_LOWER_BOUND) int lowerBound,
                @JsonProperty(FIELD_NAME_UPPER_BOUND) int upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        public int getLowerBound() {
            return lowerBound;
        }

        public int getUpperBound() {
            return upperBound;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Parallelism that = (Parallelism) o;
            return lowerBound == that.lowerBound && upperBound == that.upperBound;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lowerBound, upperBound);
        }

        @Override
        public String toString() {
            return "Parallelism{" + "lowerBound=" + lowerBound + ", upperBound=" + upperBound + '}';
        }
    }

    @JsonProperty(FIELD_NAME_PARALLELISM)
    private final Parallelism parallelism;

    @JsonProperty(FIELD_NAME_RESOURCE_PROFILE)
    private final ResourceProfile resourceProfile;

    public JustinVertexResourceRequirements(
            @JsonProperty(FIELD_NAME_PARALLELISM) Parallelism parallelism,
            @JsonProperty(FIELD_NAME_RESOURCE_PROFILE) ResourceProfile resourceProfile) {
        this.parallelism = checkNotNull(parallelism);
        this.resourceProfile = resourceProfile;
    }

    public Parallelism getParallelism() {
        return parallelism;
    }

    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JustinVertexResourceRequirements that = (JustinVertexResourceRequirements) o;
        return parallelism.equals(that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parallelism, resourceProfile);
    }

    @Override
    public String toString() {
        return "JustinVertexResourceRequirements{"
                + "parallelism="
                + parallelism
                + ", resourceProfile="
                + resourceProfile
                + '}';
    }
}
