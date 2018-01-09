package com.indeed.grpc.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.google.common.base.Strings;
import io.grpc.Attributes;
import io.grpc.NameResolverProvider;
import io.grpc.internal.GrpcUtil;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Optional;

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
                GrpcUtil.TIMER_SERVICE,
                GrpcUtil.SHARED_CHANNEL_EXECUTOR
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
}
