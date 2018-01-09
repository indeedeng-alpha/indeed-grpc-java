package com.indeed.grpc.netty;

import com.google.common.base.Strings;
import com.indeed.util.core.DataLoadingRunnable;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Implementation of a {@link DataLoadingRunnable} for an {@link SslContext}.
 *
 * After a quick code read in grpc-java, the use of the {@link SslContext}
 * supports the swap of the underlying SslContext.
 *
 * @author jpitz
 */
public class SslContextReloader extends DataLoadingRunnable implements Supplier<SslContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SslContextReloader.class);

    private final AtomicReference<SslContext> reference = new AtomicReference<>(null);
    private final SslContextReference sslContext = new SslContextReference(reference);

    private final ExceptionalSupplier<SslContext> loader;

    public SslContextReloader(
            final ExceptionalSupplier<SslContext> loader
    ) {
        this(null, loader);
    }

    public SslContextReloader(
            @Nullable final String suffix,
            final ExceptionalSupplier<SslContext> loader
    ) {
        super(computeNamespace(suffix));

        this.loader = loader;

        load(); // force the load of the data for the first time
    }

    private static String computeNamespace(@Nullable final String suffix) {
        if (Strings.isNullOrEmpty(suffix)) {
            return "SslContextReloader";
        } else {
            return "SslContextReloader-" + suffix;
        }
    }

    @Override
    public boolean load() {
        final SslContext previous = reference.get();
        try {
            final SslContext context = loader.get();
            if (context == null) {
                return finishLoadWithReloadState(ReloadState.FAILED, null);
            }

            if (Objects.equals(previous, context)) {
                return finishLoadWithReloadState(ReloadState.NO_CHANGE, null);
            }

            reference.set(context);
            return finishLoadWithReloadState(ReloadState.RELOADED, null);
        } catch (final Exception e) {
            LOGGER.error("Failed to load SslContext.", e);
        }

        return finishLoadWithReloadState(ReloadState.FAILED, null);
    }

    @Override
    public SslContext get() {
        return sslContext;
    }

    /**
     * Like {@link Supplier}, but can throw an exception.
     *
     * @param <T> The type of element being returned.
     */
    @FunctionalInterface
    public interface ExceptionalSupplier<T> {
        @Nullable
        T get() throws Exception;
    }
}
