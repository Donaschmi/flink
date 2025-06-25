package org.apache.flink.runtime.jobgraph.charon;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.jobgraph.justin.JustinVertexResourceRequirements;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class CharonResourceRequirements implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A key for an internal config option (intentionally prefixed with $internal to make this
     * explicit), that we'll serialize the {@link CharonResourceRequirements} into, when writing it
     * to {@link JobGraph}.
     */
    private static final String CHARON_RESOURCE_REQUIREMENTS_KEY =
            "$internal.charon-resource-requirements";

    private static final CharonResourceRequirements EMPTY =
            new CharonResourceRequirements(Collections.emptyMap());

    /*
     * Write {@link JustinResourceRequirements resource requirements} into the configuration of a
     * given {@link JobGraph}.
     *
     * @param jobGraph job graph to write requirements to
     * @param justinResourceRequirements resource requirements to write
     * @throws IOException in case we're not able to serialize requirements into the configuration
     */
    public static void writeToJobGraph(
            JobGraph jobGraph, CharonResourceRequirements charonResourceRequirements)
            throws IOException {
        InstantiationUtil.writeObjectToConfig(
                charonResourceRequirements,
                jobGraph.getJobConfiguration(),
                CHARON_RESOURCE_REQUIREMENTS_KEY);
    }

    /**
     * Read {@link JustinResourceRequirements resource requirements} from the configuration of a
     * given {@link JobGraph}.
     *
     * @param jobGraph job graph to read requirements from
     * @throws IOException in case we're not able to deserialize requirements from the configuration
     */
    public static Optional<JustinResourceRequirements> readFromJobGraph(JobGraph jobGraph)
            throws IOException {
        try {
            return Optional.ofNullable(
                    InstantiationUtil.readObjectFromConfig(
                            jobGraph.getJobConfiguration(),
                            CHARON_RESOURCE_REQUIREMENTS_KEY,
                            CharonResourceRequirements.class.getClassLoader()));
        } catch (ClassNotFoundException e) {
            throw new IOException(
                    "Unable to deserialize JustinResourceRequirements due to missing classes. This might happen when the JobGraph was written from a different Flink version.",
                    e);
        }
    }

    /**
     * This method validates that:
     *
     * <ul>
     *   <li>The requested boundaries are less or equal than the max parallelism.
     *   <li>The requested boundaries are greater than zero.
     *   <li>The requested upper bound is greater than the lower bound.
     *   <li>There are no unknown job vertex ids and that we're not missing any.
     * </ul>
     *
     * In case any boundary is set to {@code -1}, it will be expanded to the default value ({@code
     * 1} for the lower bound and the max parallelism for the upper bound), before the validation.
     *
     * @param charonResourceRequirements contains the new resources requirements for the job
     *     vertices
     * @param maxParallelismPerVertex allows us to look up maximum possible parallelism for a job
     *     vertex
     * @return a list of validation errors
     */
    public static List<String> validate(
            CharonResourceRequirements charonResourceRequirements,
            Map<JobVertexID, Integer> maxParallelismPerVertex) {
        final List<String> errors = new ArrayList<>();
        final Set<JobVertexID> missingJobVertexIds =
                new HashSet<>(maxParallelismPerVertex.keySet());
        for (JobVertexID jobVertexId : charonResourceRequirements.getJobVertices()) {
            missingJobVertexIds.remove(jobVertexId);
            final Optional<Integer> maybeMaxParallelism =
                    Optional.ofNullable(maxParallelismPerVertex.get(jobVertexId));
            if (maybeMaxParallelism.isPresent()) {
                final JustinVertexResourceRequirements.Parallelism requestedParallelism =
                        charonResourceRequirements.getParallelism(jobVertexId);
                int lowerBound =
                        requestedParallelism.getLowerBound() == -1
                                ? 1
                                : requestedParallelism.getLowerBound();
                int upperBound =
                        requestedParallelism.getUpperBound() == -1
                                ? maybeMaxParallelism.get()
                                : requestedParallelism.getUpperBound();
                if (lowerBound < 1 || upperBound < 1) {
                    errors.add(
                            String.format(
                                    "Both, the requested lower bound [%d] and upper bound [%d] for job vertex [%s] must be greater than zero.",
                                    lowerBound, upperBound, jobVertexId));
                    // Don't validate this vertex any further to avoid additional noise.
                    continue;
                }
                if (lowerBound > upperBound) {
                    errors.add(
                            String.format(
                                    "The requested lower bound [%d] for job vertex [%s] is higher than the upper bound [%d].",
                                    lowerBound, jobVertexId, upperBound));
                }
                if (maybeMaxParallelism.get() < upperBound) {
                    errors.add(
                            String.format(
                                    "The newly requested parallelism %d for the job vertex %s exceeds its maximum parallelism %d.",
                                    upperBound, jobVertexId, maybeMaxParallelism.get()));
                }
            } else {
                errors.add(
                        String.format(
                                "Job vertex [%s] was not found in the JobGraph.", jobVertexId));
            }
        }
        for (JobVertexID jobVertexId : missingJobVertexIds) {
            errors.add(
                    String.format(
                            "The request is incomplete, missing job vertex [%s] resource requirements.",
                            jobVertexId));
        }
        return errors;
    }

    public static CharonResourceRequirements empty() {
        return CharonResourceRequirements.EMPTY;
    }

    private final Map<JobVertexID, CharonVertexResourceRequirements> vertexResources;

    public CharonResourceRequirements(
            Map<JobVertexID, CharonVertexResourceRequirements> vertexResources) {
        this.vertexResources =
                Collections.unmodifiableMap(new HashMap<>(checkNotNull(vertexResources)));
    }

    public CharonVertexResourceRequirements.Parallelism getParallelism(JobVertexID jobVertexId) {
        return Optional.ofNullable(vertexResources.get(jobVertexId))
                .map(CharonVertexResourceRequirements::getParallelism)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "No requirement set for vertex " + jobVertexId));
    }

    public ResourceProfile getResourceProfile(JobVertexID jobVertexId) {
        return Optional.ofNullable(vertexResources.get(jobVertexId))
                .map(CharonVertexResourceRequirements::getResourceProfile)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "No requirement set for vertex " + jobVertexId));
    }

    public int getTargetTM(JobVertexID jobVertexId) {
        return Optional.ofNullable(vertexResources.get(jobVertexId))
                .map(CharonVertexResourceRequirements::getTargetTM)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "No requirement set for vertex " + jobVertexId));

    }

    public Set<JobVertexID> getJobVertices() {
        return vertexResources.keySet();
    }

    public Map<JobVertexID, CharonVertexResourceRequirements> getJobVertexParallelisms() {
        return vertexResources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CharonResourceRequirements that = (CharonResourceRequirements) o;
        return Objects.equals(vertexResources, that.vertexResources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexResources);
    }

    @Override
    public String toString() {
        return "JustinResourceRequirements{" + "vertexResources=" + vertexResources + '}';
    }
}
