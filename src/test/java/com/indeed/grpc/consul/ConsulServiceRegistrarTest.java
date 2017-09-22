package com.indeed.grpc.consul;

import com.ecwid.consul.v1.agent.AgentClient;
import com.ecwid.consul.v1.agent.model.NewService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.grpc.UtilsForTesting;
import mockit.Expectations;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;
import mockit.integration.junit4.JMockit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.indeed.grpc.UtilsForTesting.expectNullPointerException;
import static com.indeed.grpc.consul.ConsulServiceRegistrar.computeId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
@RunWith(JMockit.class)
public class ConsulServiceRegistrarTest {
    @Mocked
    private AgentClient agentClient;

    private ConsulServiceRegistrar.Builder registrarBuilder;
    private ConsulServiceRegistrar registrar;

    @Before
    public void setup() {
        registrarBuilder = ConsulServiceRegistrar.newBuilder()
                .withScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
                .withAgentClient(agentClient)
                .withHeartbeatPeriod(1)
                .withHeartbeatPeriodTimeUnit(TimeUnit.SECONDS)
                .withTags(Lists.newArrayList("tag1"))
                .withTag("tag2")
                .withExcludedServices(Sets.newHashSet("service1"))
                .withExcludedService("service2");

        registrar = registrarBuilder.build();
    }

    @Test
    public void testBuilderAccessors() {
        assertNotNull(registrarBuilder.getScheduledExecutorService());
        assertNotNull(registrarBuilder.getAgentClient());
        assertEquals(1, registrarBuilder.getHeartbeatPeriod());
        assertEquals(TimeUnit.SECONDS, registrarBuilder.getHeartbeatPeriodTimeUnit());

        final List<String> tags = registrarBuilder.getTags();
        assertNotNull(tags);
        assertEquals(2, tags.size());
        assertTrue(tags.contains("tag1"));
        assertTrue(tags.contains("tag2"));
        assertFalse(tags.contains("tag3"));

        final Set<String> excludedServices = registrarBuilder.getExcludedServices();
        assertNotNull(excludedServices);
        assertEquals(2, excludedServices.size());
        assertTrue(excludedServices.contains("service1"));
        assertTrue(excludedServices.contains("service2"));
        assertFalse(excludedServices.contains("service3"));
    }

    @Test
    public void testBuilderWithNulls() {
        expectNullPointerException(() -> registrarBuilder.withScheduledExecutorService(null));
        expectNullPointerException(() -> registrarBuilder.withAgentClient(null));
        expectNullPointerException(() -> registrarBuilder.withHeartbeatPeriodTimeUnit(null));
        expectNullPointerException(() -> registrarBuilder.withTags(null));
        expectNullPointerException(() -> registrarBuilder.withTag(null));
        expectNullPointerException(() -> registrarBuilder.withExcludedServices(null));
        expectNullPointerException(() -> registrarBuilder.withExcludedService(null));
    }

    @Test
    public void testComputeId() {
        final String id = computeId("localhost", 8080, "service1");

        assertEquals("service1::7538edebdd7fd1db", id);
    }

    @Test
    public void testRegisterServices() throws InterruptedException, IOException {
        final List<String> serviceNames = Lists.newArrayList(
                "service1", "service2", "service3", "service4"
        );

        final String advertiseAddress = "localhost";
        final int port = 8080;

        new NonStrictExpectations() {{
            agentClient.agentServiceRegister((NewService) any);
            times = 2;

            agentClient.agentCheckPass(anyString);
            minTimes = 2;
            maxTimes = 10;

            agentClient.agentServiceDeregister(anyString);
            times = 2;
        }};

        registrar.registerServices(advertiseAddress, port, serviceNames.stream());

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));

        registrar.close();
    }
}
