package com.indeed.grpc.consul;

import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.catalog.CatalogClient;
import com.ecwid.consul.v1.catalog.model.CatalogService;
import com.ecwid.consul.v1.kv.KeyValueClient;
import com.google.common.base.Strings;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import io.grpc.internal.LogExceptionRunnable;
import io.grpc.internal.SharedResourceHolder;
import io.grpc.internal.SharedResourceHolder.Resource;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNullableByDefault;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
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
 * Much of this implementation is based off of the existing {@link
 * io.grpc.internal.DnsNameResolver}
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

    private final Resource<ScheduledExecutorService> timerServiceResource;
    private final Resource<ExecutorService> executorResource;
    private final int retryInterval;
    private final TimeUnit retryIntervalTimeUnit;

    // pulled the bulk of this from the DnsNameResolver
    private @Nullable Listener listener = null;
    private @Nullable ScheduledExecutorService timerService = null;
    private @Nullable ExecutorService executor = null;
    private @Nullable ScheduledFuture<?> resolutionTask = null;
    private boolean resolving = false;
    private boolean shutdown = false;

    ConsulNameResolver(
            final CatalogClient catalogClient,
            final KeyValueClient keyValueClient,
            final String serviceName,
            final Optional<String> tag,
            final Resource<ScheduledExecutorService> timerServiceResource,
            final Resource<ExecutorService> executorResource
    ) {
        this(
                catalogClient, keyValueClient, serviceName,
                tag, timerServiceResource, executorResource,
                1, TimeUnit.MINUTES
        );
    }

    ConsulNameResolver(
            final CatalogClient catalogClient,
            final KeyValueClient keyValueClient,
            final String serviceName,
            final Optional<String> tag,
            final Resource<ScheduledExecutorService> timerServiceResource,
            final Resource<ExecutorService> executorResource,
            final int retryInterval,
            final TimeUnit retryIntervalTimeUnit
    ) {
        this.catalogClient = catalogClient;
        this.keyValueClient = keyValueClient;
        this.serviceName = serviceName;
        this.tag = tag;
        this.timerServiceResource = timerServiceResource;
        this.executorResource = executorResource;

        this.retryInterval = retryInterval;
        this.retryIntervalTimeUnit = retryIntervalTimeUnit;
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
    public final synchronized void start(final Listener listener) {
        checkState(this.listener == null, "ConsulNameResolver already started");
        timerService = SharedResourceHolder.get(timerServiceResource);
        executor = SharedResourceHolder.get(executorResource);
        this.listener = checkNotNull(listener, "listener cannot be null");
        resolve();
    }

    @Override
    public final synchronized void refresh() {
        checkState(listener != null, "ConsulNameResolver not yet started");
        resolve();
    }

    private void resolve() {
        if (shutdown || resolving) {
            return;
        }
        executor.execute(this::run);
    }

    private synchronized void run() {
        if (resolutionTask != null) {
            resolutionTask.cancel(false);
            resolutionTask = null;
        }

        if (shutdown) {
            return;
        }

        checkNotNull(listener, "resolver not started");
        checkNotNull(timerService, "resolver not started");
        checkNotNull(executor, "resolver not started");

        resolving = true;
        try {
            final Response<List<CatalogService>> response = tag
                    .map(tag -> catalogClient.getCatalogService(serviceName, tag, QueryParams.DEFAULT))
                    .orElseGet(() -> catalogClient.getCatalogService(serviceName, QueryParams.DEFAULT));

            final List<EquivalentAddressGroup> servers = response.getValue().stream()
                    .map((service) -> {
                        // use service address then fall back to address
                        String host = service.getServiceAddress();
                        if (Strings.isNullOrEmpty(host)) {
                            host = service.getAddress();
                        }

                        final int port = service.getServicePort();

                        final SocketAddress address = new InetSocketAddress(host, port);

                        return new EquivalentAddressGroup(address);
                    })
                    .collect(Collectors.toList());

            listener.onAddresses(servers, Attributes.EMPTY);
        } catch (final Exception e) {
            if (shutdown) {
                return;
            }

            resolutionTask = timerService.schedule(
                    new LogExceptionRunnable(this::run),
                    retryInterval, retryIntervalTimeUnit
            );

            listener.onError(Status.UNAVAILABLE.withCause(e));
        } finally {
            resolving = false;
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

        if (timerService != null) {
            timerService = SharedResourceHolder.release(timerServiceResource, timerService);
        }

        if (executor != null) {
            executor = SharedResourceHolder.release(executorResource, executor);
        }
    }
}
