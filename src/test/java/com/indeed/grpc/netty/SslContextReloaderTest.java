package com.indeed.grpc.netty;

import com.indeed.util.core.DataLoadingRunnable.ReloadState;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class SslContextReloaderTest {
    @Test
    public void testNewInstanceLoader() throws Exception {
        final SslContextReloader reloader = new SslContextReloader(() -> {
            return new JdkSslContext(SSLContext.getDefault(), true, ClientAuth.REQUIRE);
        });

        assertTrue(reloader.load());
        assertEquals(ReloadState.RELOADED, reloader.getReloadState());
        assertNull(reloader.getDataVersion());
    }

    @Test
    public void testStaticInstanceLoader() throws Exception {
        final JdkSslContext context = new JdkSslContext(SSLContext.getDefault(), true, ClientAuth.REQUIRE);
        final SslContextReloader reloader = new SslContextReloader(() -> context);

        // don't invoke load here because the constructor forces load the first time
        assertEquals(ReloadState.RELOADED, reloader.getReloadState());
        assertNull(reloader.getDataVersion());

        assertFalse(reloader.load());
        assertEquals(ReloadState.NO_CHANGE, reloader.getReloadState());
        assertNull(reloader.getDataVersion());
    }

    @Test
    public void testExceptionalLoader() throws Exception {
        final SslContextReloader reloader = new SslContextReloader(() -> {
            throw new RuntimeException("blah");
        });

        assertFalse(reloader.load());
        assertEquals(ReloadState.FAILED, reloader.getReloadState());
        assertNull(reloader.getDataVersion());
    }

    @Test
    public void get() throws Exception {
        final SslContextReloader reloader = new SslContextReloader(() -> null);

        assertFalse(reloader.load());
        assertEquals(ReloadState.FAILED, reloader.getReloadState());
        assertNull(reloader.getDataVersion());

        final SslContext context = reloader.get();
        assertNotNull(context);

        expectNullPointerException(context::isClient);
        expectNullPointerException(context::cipherSuites);
        expectNullPointerException(context::sessionCacheSize);
        expectNullPointerException(context::sessionTimeout);
        expectNullPointerException(context::applicationProtocolNegotiator);
        expectNullPointerException(context::sessionContext);
        expectNullPointerException(() -> context.newEngine(ByteBufAllocator.DEFAULT));
        expectNullPointerException(() -> context.newEngine(ByteBufAllocator.DEFAULT, "localhost", 1234));
    }

    private void expectNullPointerException(
            final Supplier<?> supplier
    ) {
        try {
            supplier.get();
            fail("Expected null pointer exception, but one was not thrown");
        } catch (final NullPointerException ignored) {}
    }
}