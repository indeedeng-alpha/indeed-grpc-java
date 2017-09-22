package com.indeed.grpc;

import static org.junit.Assert.fail;

/**
 *
 */
public final class UtilsForTesting {
    private UtilsForTesting() {}

    /**
     * Expect a NullPointerException. If one isn't thrown, fail the unit test.
     *
     * @param runnable A runnable that should throw a null pointer exception.
     */
    public static void expectNullPointerException(
            final Runnable runnable
    ) {
        try {
            runnable.run();
            fail("Expected null pointer exception, but one was not thrown");
        } catch (final NullPointerException ignored) {}
    }
}
