package com.indeed.grpc.consul;

import io.grpc.Attributes;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class ConsulNameResolverProviderTest {
    public static final ConsulNameResolverProvider provider = new ConsulNameResolverProvider();

    @Test
    public void testGetDefaultScheme() throws Exception {
        assertEquals("consul", provider.getDefaultScheme());
    }

    @Test
    public void testIsAvailable() {
        assertTrue(provider.isAvailable());
    }

    @Test
    public void testPriority() {
        assertEquals(5, provider.priority());
    }

    @Test
    public void testNewNameResolver() {
        final Attributes empty = Attributes.EMPTY;

        final URI invalidScheme = URI.create("zk:///");
        assertNull(provider.newNameResolver(invalidScheme, empty));

        final URI missingService = URI.create("consul:///");
        try {
            provider.newNameResolver(missingService, empty);
            fail("no service provided, cannot resolve");
        } catch (final IllegalArgumentException ignored) {}

        final URI defaultAddress = URI.create("consul:///MyServiceName");
        {
            final ConsulNameResolver r = provider.newNameResolver(defaultAddress, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertNull(r.getTag());
        }

        final URI defaultAddressWithTag = URI.create("consul:///MyServiceName#grpc");
        {
            final ConsulNameResolver r = provider.newNameResolver(defaultAddressWithTag, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertEquals("grpc", r.getTag());
        }

        final URI hostAddress = URI.create("consul://localhost/MyServiceName");
        {
            final ConsulNameResolver r = provider.newNameResolver(hostAddress, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertNull(r.getTag());
        }

        final URI hostAddressWithTag = URI.create("consul://localhost/MyServiceName#grpc");
        {
            final ConsulNameResolver r = provider.newNameResolver(hostAddressWithTag, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertEquals("grpc", r.getTag());
        }

        final URI hostAndPortAddress = URI.create("consul://localhost:8500/MyServiceName");
        {
            final ConsulNameResolver r = provider.newNameResolver(hostAndPortAddress, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertNull(r.getTag());
        }

        final URI hostAndPortAddressWithTag = URI.create("consul://localhost:8500/MyServiceName#grpc");
        {
            final ConsulNameResolver r = provider.newNameResolver(hostAndPortAddressWithTag, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertEquals("grpc", r.getTag());
        }
    }
}