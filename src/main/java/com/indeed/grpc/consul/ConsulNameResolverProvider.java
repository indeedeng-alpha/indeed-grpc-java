package com.indeed.grpc.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.google.common.base.Strings;
import io.grpc.Attributes;
import io.grpc.NameResolverProvider;
import io.grpc.internal.GrpcUtil;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A provider for {@link ConsulNameResolver}.
 *
 * <ul>
 *     <li>{@code "consul:///MyServiceName"} (using default address)</li>
 *     <li>{@code "consul://localhost/MyServiceName"} (using specified host, default port)</li>
 *     <li>{@code "consul://localhost:8500/MyServiceName"} (using specified host and port)<li>
 *     <li>{@code "consul://localhost:8500/MyServiceName#grpc"} (using specified host, port, and tag)</li>
 * </ul>
 *
 * Loosely based off of the io.grpc.internal.DnsNameResolverProvider.
 *
 * @author jpitz
 */
public final class ConsulNameResolverProvider extends NameResolverProvider {
    private static final String SCHEME = "consul";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 8500;

    private final ScheduledExecutorService timerService;
    private final int resolveInterval;
    private final TimeUnit resolveIntervalTimeUnit;

    @Deprecated
    public ConsulNameResolverProvider() {
        this(builder());
    }

    ConsulNameResolverProvider(
            final Builder builder
    ) {
        this.timerService = builder.timerService;
        this.resolveInterval = builder.resolveInterval;
        this.resolveIntervalTimeUnit = builder.resolveIntervalTimeUnit;
    }

    @Nullable
    @Override
    public ConsulNameResolver newNameResolver(final URI targetUri, final Attributes params) {
        if (!SCHEME.equals(targetUri.getScheme())) {
            return null;
        }

        final String targetPath = checkNotNull(targetUri.getPath(), "targetPath");
        checkArgument(targetPath.startsWith("/"));

        final String serviceName = targetPath.substring(1);
        checkArgument(serviceName.length() > 0, "serviceName");

        String consulHost = targetUri.getHost();
        if (Strings.isNullOrEmpty(consulHost)) {
            consulHost = DEFAULT_HOST;
        }

        int consulPort = targetUri.getPort();
        if (consulPort == -1) {
            consulPort = DEFAULT_PORT;
        }

        final String tag = Strings.emptyToNull(targetUri.getFragment());

        final ConsulClient consulClient = ConsulClientManager.getInstance(consulHost, consulPort);

        return new ConsulNameResolver(
                consulClient /* CatalogClient */,
                consulClient /* KeyValueClient */,
                serviceName,
                Optional.ofNullable(tag),
                timerService,
                resolveInterval,
                resolveIntervalTimeUnit
        );
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 5;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
        private int resolveInterval = 1;
        private TimeUnit resolveIntervalTimeUnit = TimeUnit.MINUTES;

        private Builder() {}

        public ScheduledExecutorService getTimerService() {
            return timerService;
        }

        public void setTimerService(final ScheduledExecutorService timerService) {
            this.timerService = timerService;
        }

        public Builder withTimerService(final ScheduledExecutorService timerService) {
            setTimerService(timerService);
            return this;
        }

        public int getResolveInterval() {
            return resolveInterval;
        }

        public void setResolveInterval(final int resolveInterval) {
            this.resolveInterval = resolveInterval;
        }

        public Builder withResolveInterval(final int resolveInterval) {
            setResolveInterval(resolveInterval);
            return this;
        }

        public TimeUnit getResolveIntervalTimeUnit() {
            return resolveIntervalTimeUnit;
        }

        public void setResolveIntervalTimeUnit(final TimeUnit resolveIntervalTimeUnit) {
            this.resolveIntervalTimeUnit = resolveIntervalTimeUnit;
        }

        public Builder withResolveIntervalTimeUnit(final TimeUnit resolveIntervalTimeUnit) {
            setResolveIntervalTimeUnit(resolveIntervalTimeUnit);
            return this;
        }

        public ConsulNameResolverProvider build() {
            return new ConsulNameResolverProvider(this);
        }
    }
}
