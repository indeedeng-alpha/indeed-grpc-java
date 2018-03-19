package com.indeed.grpc.consul;

import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.catalog.CatalogClient;
import com.ecwid.consul.v1.catalog.model.CatalogService;
import com.ecwid.consul.v1.kv.KeyValueClient;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import io.grpc.internal.LogExceptionRunnable;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A consul-based {@link NameResolver}.
 *
 * The {@link EquivalentAddressGroup} will be fetched from the service catalog.
 * When the Java implementation supports service configurations, we can fetch
 * those from the key-value store.
 *
 * Much of this implementation is based off of the existing io.grpc.internal.DnsNameResolver
 *
 * @see ConsulNameResolverProvider
 *
 * @author jpitz
 */
public final class ConsulNameResolver extends NameResolver {
    private final CatalogClient catalogClient;
    private final KeyValueClient keyValueClient;

    private final String serviceName;
    private final Optional<String> tag;

    private final ScheduledExecutorService timerService;
    private final int resolveInterval;
    private final TimeUnit resolveIntervalTimeUnit;

    @Nullable
    private Listener listener = null;

    @Nullable
    private ScheduledFuture<?> resolutionTask = null;

    private boolean shutdown = false;

    private Set<HostAndPort> knownServiceAddresses = null;

    ConsulNameResolver(
            final CatalogClient catalogClient,
            final KeyValueClient keyValueClient,
            final String serviceName,
            final Optional<String> tag,
            final ScheduledExecutorService timerService,
            final int resolveInterval,
            final TimeUnit resolveIntervalTimeUnit
    ) {
        this.catalogClient = catalogClient;
        this.keyValueClient = keyValueClient;
        this.serviceName = serviceName;
        this.tag = tag;
        this.timerService = timerService;
        this.resolveInterval = resolveInterval;
        this.resolveIntervalTimeUnit = resolveIntervalTimeUnit;
    }

    @Nullable
    public String getTag() {
        return tag.orElse(null);
    }

    /**
     * TODO: Keep an eye on this call stack.
     *
     * Right now, the call stack doesn't do anything with this, but it seems
     * like it expects it to be an authoritative address (i.e. host and port).
     *
     * From a quick stack trace, the target uri that's constructed is used as
     * a key to a map in auth land.
     *
     * @return The service authority.
     */
    @Override
    public String getServiceAuthority() {
        return serviceName;
    }

    @Override
    public synchronized void start(final Listener listener) {
        checkState(this.listener == null, "ConsulNameResolver already started");
        this.listener = checkNotNull(listener, "listener cannot be null");
        this.resolutionTask = timerService.scheduleAtFixedRate(
                new LogExceptionRunnable(this::run),
                0, resolveInterval, resolveIntervalTimeUnit
        );
    }

    @Override
    public synchronized void refresh() {
        checkState(listener != null, "ConsulNameResolver not yet started");
    }

    private synchronized void run() {
        if (shutdown) {
            return;
        }

        checkNotNull(listener, "resolver not started");
        checkNotNull(timerService, "resolver not started");

        try {
            final Response<List<CatalogService>> response = tag
                    .map(tag -> catalogClient.getCatalogService(serviceName, tag, QueryParams.DEFAULT))
                    .orElseGet(() -> catalogClient.getCatalogService(serviceName, QueryParams.DEFAULT));

            final Set<HostAndPort> readAddressList = response.getValue().stream()
                    .map((service) -> {
                        // use service address then fall back to address
                        String host = service.getServiceAddress();
                        if (Strings.isNullOrEmpty(host)) {
                            host = service.getAddress();
                        }

                        final int port = service.getServicePort();

                        return HostAndPort.fromParts(host, port);
                    }).collect(Collectors.toSet());

            if (!readAddressList.equals(knownServiceAddresses)) {
                knownServiceAddresses = readAddressList;

                final List<EquivalentAddressGroup> servers = readAddressList.stream()
                        .map((hostAndPort) -> {
                            final SocketAddress address = new InetSocketAddress(
                                    hostAndPort.getHostText(),
                                    hostAndPort.getPort()
                            );

                            return new EquivalentAddressGroup(address);
                        }).collect(Collectors.toList());

                listener.onAddresses(servers, Attributes.EMPTY);
            }
        } catch (final Exception e) {
            if (shutdown) {
                return;
            }

            // only report error if we have no list
            if (knownServiceAddresses.isEmpty()) {
                listener.onError(Status.UNAVAILABLE.withCause(e));
            }
        }
    }

    @Override
    public final synchronized void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;

        if (resolutionTask != null) {
            resolutionTask.cancel(false);
            resolutionTask = null;
        }

        // intentionally not shutting down the timer service since it's a shared resource.
    }
}
