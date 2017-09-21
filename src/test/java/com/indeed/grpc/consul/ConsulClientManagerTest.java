package com.indeed.grpc.consul;

import com.ecwid.consul.v1.ConsulClient;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Only testing plaintext because construction of the tls client throws an exception.
 *
 * @author jpitz
 */
public class ConsulClientManagerTest {
    @Test
    public void getInstance() throws Exception {
        final ConsulClient c1 = ConsulClientManager.getInstance("localhost", 8500);
        final ConsulClient c2 = ConsulClientManager.getInstance("localhost", 8500);
        final ConsulClient c3 = ConsulClientManager.getInstance("127.0.0.1", 8500);

        assertTrue(c1 == c2);
        assertFalse(c2 == c3);
    }
}