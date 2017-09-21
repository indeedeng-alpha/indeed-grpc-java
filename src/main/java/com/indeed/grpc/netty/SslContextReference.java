package com.indeed.grpc.netty;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSessionContext;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Extension of the {@link SslContext} that allows for the underlying
 * {@link SslContext} to be reloaded or swapped.
 *
 * @author jpitz
 */
final class SslContextReference extends SslContext {
    private final AtomicReference<SslContext> ssl;

    SslContextReference(
            final AtomicReference<SslContext> ssl
    ) {
        this.ssl = ssl;
    }

    @Override
    public boolean isClient() {
        return ssl.get().isClient();
    }

    @Override
    public List<String> cipherSuites() {
        return ssl.get().cipherSuites();
    }

    @Override
    public long sessionCacheSize() {
        return ssl.get().sessionCacheSize();
    }

    @Override
    public long sessionTimeout() {
        return ssl.get().sessionTimeout();
    }

    @Override
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return ssl.get().applicationProtocolNegotiator();
    }

    @Override
    public SSLEngine newEngine(final ByteBufAllocator alloc) {
        return ssl.get().newEngine(alloc);
    }

    @Override
    public SSLEngine newEngine(final ByteBufAllocator alloc, final String peerHost, final int peerPort) {
        return ssl.get().newEngine(alloc, peerHost, peerPort);
    }

    @Override
    public SSLSessionContext sessionContext() {
        return ssl.get().sessionContext();
    }
}
