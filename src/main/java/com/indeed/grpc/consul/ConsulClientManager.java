package com.indeed.grpc.consul;

import com.ecwid.consul.transport.TLSConfig;
import com.ecwid.consul.v1.ConsulClient;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple manager class that will manage instances of the {@link ConsulClient}.
 * The ConsulClient is thread safe so maintaining an index of ConsulClient
 * instances will reduce extra object cruft by sharing a singleton for a given
 * {@code host} / {@code port} / {@link TLSConfig} triplet.
 *
 * TODO: This should probably be moved to the ecwid library for everyone to share.
 *
 * @author jpitz
 */
@ThreadSafe
public final class ConsulClientManager {
    private static final ConsulClientManager INSTANCE = new ConsulClientManager();

    private final ConcurrentMap<IndexKey, ConsulClient> index = new ConcurrentHashMap<>();

    private ConsulClientManager() {}

    /**
     * Get or create a plaintext {@link ConsulClient} for the given {@code
     * host} / {@code port} pair.
     *
     * @param host The hostname of the consul instance we are attempting to
     *             connect to.
     * @param port The port of the consul instance we are attempting to connect
     *             to.
     * @return The consul client for the given triplet.
     */
    public static ConsulClient getInstance(
            final String host,
            final int port
    ) {
        return getInstance(host, port, null);
    }

    /**
     * Get or create the {@link ConsulClient} instance for the given {@code
     * host} / {@code port} / {@link TLSConfig} triplet.
     *
     * @param host The hostname of the consul instance we are attempting to
     *             connect to.
     * @param port The port of the consul instance we are attempting to connect
     *             to.
     * @param tlsConfig The TLS configuration used to secure communication with
     *                  the consul instance.
     * @return The consul client for the given triplet.
     */
    public static ConsulClient getInstance(
            final String host,
            final int port,
            @Nullable final TLSConfig tlsConfig
    ) {
        return INSTANCE.index.computeIfAbsent(new IndexKey(host, port, tlsConfig), (key) -> {
            if (tlsConfig == null) {
                return new ConsulClient(host, port);
            } else {
                return new ConsulClient(host, port, tlsConfig);
            }
        });
    }

    /**
     * Key for the map.
     *
     * TODO: TLSConfig does not implement equals or hash code. Needs to be addressed.
     */
    private static final class IndexKey {
        private final String host;
        private final int port;
        private final @Nullable TLSConfig tlsConfig;

        private IndexKey(
                final String host,
                final int port,
                @Nullable final TLSConfig tlsConfig
        ) {
            this.host = host;
            this.port = port;
            this.tlsConfig = tlsConfig;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final IndexKey indexKey = (IndexKey) o;
            return port == indexKey.port &&
                    Objects.equals(host, indexKey.host) &&
                    Objects.equals(tlsConfig, indexKey.tlsConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port, tlsConfig);
        }
    }
}
