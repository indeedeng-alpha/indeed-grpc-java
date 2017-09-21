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
 * @author jpitz
 */
public class ConsulNameResolverProviderTest {
    private static final ConsulNameResolverProvider PROVIDER = new ConsulNameResolverProvider();

    @Test
    public void testGetDefaultScheme() throws Exception {
        assertEquals("consul", PROVIDER.getDefaultScheme());
    }

    @Test
    public void testIsAvailable() {
        assertTrue(PROVIDER.isAvailable());
    }

    @Test
    public void testPriority() {
        assertEquals(5, PROVIDER.priority());
    }

    @Test
    public void testNewNameResolver() {
        final Attributes empty = Attributes.EMPTY;

        final URI invalidScheme = URI.create("zk:///");
        assertNull(PROVIDER.newNameResolver(invalidScheme, empty));

        final URI missingService = URI.create("consul:///");
        try {
            PROVIDER.newNameResolver(missingService, empty);
            fail("no service provided, cannot resolve");
        } catch (final IllegalArgumentException ignored) {}

        final URI defaultAddress = URI.create("consul:///MyServiceName");
        {
            final ConsulNameResolver r = PROVIDER.newNameResolver(defaultAddress, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertNull(r.getTag());
        }

        final URI defaultAddressWithTag = URI.create("consul:///MyServiceName#grpc");
        {
            final ConsulNameResolver r = PROVIDER.newNameResolver(defaultAddressWithTag, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertEquals("grpc", r.getTag());
        }

        final URI hostAddress = URI.create("consul://localhost/MyServiceName");
        {
            final ConsulNameResolver r = PROVIDER.newNameResolver(hostAddress, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertNull(r.getTag());
        }

        final URI hostAddressWithTag = URI.create("consul://localhost/MyServiceName#grpc");
        {
            final ConsulNameResolver r = PROVIDER.newNameResolver(hostAddressWithTag, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertEquals("grpc", r.getTag());
        }

        final URI hostAndPortAddress = URI.create("consul://localhost:8500/MyServiceName");
        {
            final ConsulNameResolver r = PROVIDER.newNameResolver(hostAndPortAddress, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertNull(r.getTag());
        }

        final URI hostAndPortAddressWithTag = URI.create("consul://localhost:8500/MyServiceName#grpc");
        {
            final ConsulNameResolver r = PROVIDER.newNameResolver(hostAndPortAddressWithTag, empty);
            assertNotNull(r);
            assertEquals("MyServiceName", r.getServiceAuthority());
            assertEquals("grpc", r.getTag());
        }
    }
}