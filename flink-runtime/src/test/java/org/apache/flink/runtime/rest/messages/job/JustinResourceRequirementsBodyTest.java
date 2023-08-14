package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.rest.messages.RestRequestMarshallingTestBase;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsBody;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for the marshalling of {@link JustinResourceRequirementsBodyTest}. */
public class JustinResourceRequirementsBodyTest
        extends RestRequestMarshallingTestBase<JustinResourceRequirementsBody> {
    @Override
    protected Class<JustinResourceRequirementsBody> getTestRequestClass() {
        return JustinResourceRequirementsBody.class;
    }

    @Override
    protected JustinResourceRequirementsBody getTestRequestInstance() throws Exception {
        return new JustinResourceRequirementsBody(
                JustinResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(new JobVertexID(), 1, 42, null)
                        .setParallelismForJobVertex(new JobVertexID(), 1, 1337, null)
                        .build());
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            JustinResourceRequirementsBody expected, JustinResourceRequirementsBody actual) {
        assertThat(expected, equalsChangeJobRequestBody(actual));
    }

    private EqualityChangeJobRequestBodyMatcher equalsChangeJobRequestBody(
            JustinResourceRequirementsBody actual) {
        return new EqualityChangeJobRequestBodyMatcher(actual);
    }

    private static final class EqualityChangeJobRequestBodyMatcher
            extends TypeSafeMatcher<JustinResourceRequirementsBody> {

        private final JustinResourceRequirementsBody actualJobResourceRequirementsBody;

        private EqualityChangeJobRequestBodyMatcher(
                JustinResourceRequirementsBody actualJobResourceRequirementsBody) {
            this.actualJobResourceRequirementsBody = actualJobResourceRequirementsBody;
        }

        @Override
        protected boolean matchesSafely(
                JustinResourceRequirementsBody jobResourceRequirementsBody) {
            final Optional<JustinResourceRequirements> maybeActualJobResourceRequirements =
                    actualJobResourceRequirementsBody.asJobResourceRequirements();
            final Optional<JustinResourceRequirements> maybeJobResourceRequirements =
                    jobResourceRequirementsBody.asJobResourceRequirements();
            if (maybeActualJobResourceRequirements.isPresent()
                    ^ maybeJobResourceRequirements.isPresent()) {
                return false;
            }
            return maybeActualJobResourceRequirements
                    .map(actual -> actual.equals(maybeJobResourceRequirements.get()))
                    .orElse(true);
        }

        @Override
        public void describeTo(Description description) {}
    }
}
