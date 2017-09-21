package com.indeed.grpc.consul;

import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.catalog.CatalogClient;
import com.ecwid.consul.v1.catalog.model.CatalogService;
import com.ecwid.consul.v1.kv.KeyValueClient;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.ResolvedServerInfoGroup;
import io.grpc.Status;
import io.grpc.internal.GrpcUtil;
import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
@RunWith(JMockit.class)
public class ConsulNameResolverTest {
    private static final String serviceName = "MyServiceName";

    private static final long consulIndex = 1;
    private static final boolean consulKnownLeader = true;
    private static final long consulLastContact = 1;

    private static <T> Response<T> composeResponse(final T value) {
        return new Response<T>(value, consulIndex, consulKnownLeader, consulLastContact);
    }

    @Mocked
    private CatalogClient catalogClient;

    @Mocked
    private KeyValueClient keyValueClient;

    private ConsulNameResolver resolver;

    @Before
    public void setup() {
        resolver = new ConsulNameResolver(
                catalogClient,
                keyValueClient,
                serviceName,
                Optional.empty(),
                GrpcUtil.TIMER_SERVICE,
                GrpcUtil.SHARED_CHANNEL_EXECUTOR,
                1, TimeUnit.SECONDS
        );
    }

    @Test
    public void testGetServiceAuthority() {
        assertEquals(serviceName, resolver.getServiceAuthority());
    }

    @Test(expected = IllegalStateException.class)
    public void testRefreshBeforeStart() {
        resolver.refresh();
    }

    @Test
    public void testEmptyResolution() throws Exception {
        final Response<List<CatalogService>> response = composeResponse(new ArrayList<>());

        new Expectations() {{
            catalogClient.getCatalogService(serviceName, QueryParams.DEFAULT);
            result = response;
            times = 1;
        }};

        final List<NameResolverEvent<?>> events = runTest(resolver, 1);
        assertEquals(1, events.size());

        final NameResolverEvent e  = events.get(0);
        assertEquals(NameResolverEventType.ON_ADDRESSES, e.type);
        assertTrue(((List) e.payload).isEmpty());
    }

    @Test
    public void testFixedResolution() throws Exception {
        final CatalogService service = new CatalogService();
        service.setAddress("localhost");
        service.setServicePort(8080);

        final List<CatalogService> services = new ArrayList<>();
        services.add(service);

        final Response<List<CatalogService>> response = composeResponse(services);

        new Expectations() {{
            catalogClient.getCatalogService(serviceName, QueryParams.DEFAULT);
            result = response;
            times = 1;
        }};

        final List<NameResolverEvent<?>> events = runTest(resolver, 1);
        assertEquals(1, events.size());

        final NameResolverEvent e  = events.get(0);
        assertEquals(NameResolverEventType.ON_ADDRESSES, e.type);

        final List<EquivalentAddressGroup> addressGroups = (List<EquivalentAddressGroup>) e.payload;
        assertEquals(1, addressGroups.size());

        final List<SocketAddress> addresses = addressGroups.get(0).getAddresses();
        assertEquals(1, addresses.size());

        final InetSocketAddress inetAddress = (InetSocketAddress) addresses.get(0);
        assertEquals("localhost", inetAddress.getHostName());
        assertEquals(8080, inetAddress.getPort());
    }

    @Test
    public void testFailingResolution() throws Exception {
        new Expectations() {{
            catalogClient.getCatalogService(serviceName, QueryParams.DEFAULT);
            result = new RuntimeException();
            maxTimes = 5;
        }};

        final List<NameResolverEvent<?>> events = runTest(resolver, 5);

        // allow for an off by 1
        assertTrue(events.size() >= 4);
        assertTrue(events.size() <= 5);

        for (final NameResolverEvent<?> event : events) {
            assertEquals(NameResolverEventType.ON_ERROR, event.type);

            final Status s = (Status) event.payload;
            assertEquals(Status.UNAVAILABLE.getCode(), s.getCode());
            assertTrue(s.getCause() instanceof RuntimeException);
        }
    }

    private static List<NameResolverEvent<?>> runTest(
            final NameResolver resolver,
            final int sleepSeconds
    ) throws InterruptedException {
        final List<NameResolverEvent<?>> events = new ArrayList<>();

        resolver.start(new NameResolver.Listener() {
            @Override
            public void onUpdate(final List<ResolvedServerInfoGroup> list, final Attributes attributes) {
                throw new UnsupportedOperationException("You should call onAddresses instead of onUpdate");
            }

            @Override
            public void onAddresses(final List<EquivalentAddressGroup> list, final Attributes attributes) {
                events.add(new NameResolverEvent<>(NameResolverEventType.ON_ADDRESSES, list));
            }

            @Override
            public void onError(final Status status) {
                events.add(new NameResolverEvent<>(NameResolverEventType.ON_ERROR, status));
            }
        });

        Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
        resolver.shutdown();

        return events;
    }

    private enum NameResolverEventType {
        ON_ADDRESSES,
        ON_ERROR
    }

    private static final class NameResolverEvent<T> {
        final NameResolverEventType type;
        final T payload;

        public NameResolverEvent(
                final NameResolverEventType type,
                final T payload
        ) {
            this.type = type;
            this.payload = payload;
        }
    }

}