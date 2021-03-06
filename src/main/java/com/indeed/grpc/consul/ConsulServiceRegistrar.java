package com.indeed.grpc.consul;

import com.ecwid.consul.v1.agent.AgentClient;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.agent.model.NewService.Check;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.indeed.grpc.ServiceRegistrar;
import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Implementation is loosely based on the one in the spring-cloud-consul.
 *
 * This implementation uses the {@link AgentClient} to register services with
 * the local consul agent. It then enables a heartbeat by default to ensure the
 * service remains registered in consul.
 *
 * Also see:
 * https://www.consul.io/api/agent/service.html#register-service
 * https://www.consul.io/api/agent/check.html#ttl-check-pass
 *
 * @author jpitz
 */
public final class ConsulServiceRegistrar implements ServiceRegistrar, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulServiceRegistrar.class);

    private final Map<String, ScheduledFuture> servicePingers = new ConcurrentHashMap<>();
    private final Set<String> servicesWithoutPingers = Sets.newConcurrentHashSet();

    private final ScheduledExecutorService scheduledExecutorService;
    private final AgentClient agentClient;
    private final int heartbeatPeriod;
    private final TimeUnit heartbeatPeriodTimeUnit;
    private final List<String> tags;
    private final Set<String> excludedServices;
    private final List<Check> checks;
    private final boolean usingTtlCheck;
    private final String consulToken;

    private ConsulServiceRegistrar(
            final ScheduledExecutorService scheduledExecutorService,
            final AgentClient agentClient,
            final int heartbeatPeriod,
            final TimeUnit heartbeatPeriodTimeUnit,
            final List<String> tags,
            final Set<String> excludedServices,
            final List<Check> checks,
            final String consulToken
    ) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.agentClient = agentClient;
        this.heartbeatPeriod = heartbeatPeriod;
        this.heartbeatPeriodTimeUnit = heartbeatPeriodTimeUnit;
        this.tags = Lists.newArrayList(tags);
        this.excludedServices = Sets.newHashSet(excludedServices);
        this.checks = Lists.newArrayList(checks);
        this.usingTtlCheck = checks.stream().anyMatch((check) -> !isNullOrEmpty(check.getTtl()));
        this.consulToken = consulToken;
    }

    /**
     * Pull all the specified services from the provided server and register
     * them into consul. Services will be registered using the provided
     * {@code advertiseAddress}. The {@code port} will be obtained from the
     * provided {@code server}. Service names are filtered through the
     * {@link #excludedServices} set before being registered into consul.
     *
     * @param advertiseAddress The address that the service is advertising on.
     * @param server The server to pull the services and port from.
     *
     * @deprecated in favor of custom naming
     * @see #registerService(String, String, int)
     */
    @Deprecated
    public void registerServices(final String advertiseAddress, final Server server) {
        final int port = server.getPort();

        final Stream<String> services = server.getImmutableServices().stream()
                .map((ssd) -> ssd.getServiceDescriptor().getName());

        registerServices(advertiseAddress, port, services);
    }

    @Deprecated
    @VisibleForTesting
    void registerServices(final String advertiseAddress, final int port, final Stream<String> services) {
        services.filter((name) -> !excludedServices.contains(name))
                .forEach((name) -> registerService(name, advertiseAddress, port));
    }

    @Override
    public void registerService(final String serviceName, final String advertiseAddress, final int port) {
        checkNotNull(Strings.emptyToNull(advertiseAddress), "advertiseAddress");
        checkNotNull(Strings.emptyToNull(serviceName), "serviceName");

        final String id = computeId(advertiseAddress, port, serviceName);

        final NewService newService = new NewService();
        newService.setId(id);
        newService.setName(serviceName);
        newService.setTags(tags);
        newService.setAddress(advertiseAddress);
        newService.setPort(port);
        newService.setChecks(checks);

        agentClient.agentServiceRegister(newService, consulToken);

        // only set up the heartbeat if we're using a TTL check
        final ScheduledFuture future;
        if (usingTtlCheck) {
            future = scheduledExecutorService.scheduleAtFixedRate(
                    () -> heartbeat(id), heartbeatPeriod, heartbeatPeriod, heartbeatPeriodTimeUnit
            );

            final ScheduledFuture previous = servicePingers.put(id, future);
            if (previous != null) {
                previous.cancel(true);
            }
        } else {
            servicesWithoutPingers.add(id);
        }
    }

    /**
     * Using the given id, force the agent to pass the check for the service.
     * This will trigger the service TTL in consul. This bit of code was found
     * in the depths of the spring-cloud-consul source where they do the same
     * logic for the whole application using the lifecycle manager.
     *
     * @param id The id of the service.
     */
    private void heartbeat(final String id) {
        LOGGER.trace("Heartbeating service with id [" + id + "] in consul");
        try {
            agentClient.agentCheckPass("service:" + id, consulToken);
        } catch (final Throwable e) {
            LOGGER.error("Failed to register service with id [" + id + "] into consul", e);
        }
    }

    /**
     * Removes the service identified by the provided id from consul.
     *
     * @param id The id of the service.
     */
    @VisibleForTesting
    void deregisterService(final String id) {
        agentClient.agentServiceDeregister(id, consulToken);

        final ScheduledFuture future = servicePingers.remove(id);
        if (future != null) {
            future.cancel(true);
        }
    }

    @Override
    public void close() throws IOException {
        servicePingers.keySet().forEach(this::deregisterService);
        servicesWithoutPingers.forEach(this::deregisterService);
    }



    /**
     * Static utility function used to compute an identifier for the given
     * service. The format for the id is as follows:
     *
     * <pre>serviceName::sha256uid</pre>
     *
     * where the {@code sha256uid} is a sha256 of:
     *
     * <ul>
     *     <li>advertiseAddress</li>
     *     <li>port</li>
     *     <li>serviceName</li>
     * </ul>
     *
     * represented as a long, and converted to a hex string.
     *
     * @param advertiseAddress The address that the service is advertising on.
     * @param port The port where the service is listening.
     * @param serviceName The name of the service.
     * @return The string identifier for the service.
     */
    @VisibleForTesting
    static String computeId(
            final String advertiseAddress,
            final int port,
            final String serviceName
    ) {
        final Hasher hasher = SHA256.newHasher();
        hasher.putString(advertiseAddress, Charsets.UTF_8);
        hasher.putInt(port);
        hasher.putString(serviceName, Charsets.UTF_8);

        return serviceName + ":" + Long.toHexString(hasher.hash().asLong());
    }
    private static final HashFunction SHA256 = Hashing.sha256();



    /**
     * @return A new builder used to compose and tune a registrar.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder object used to compose the {@link ConsulServiceRegistrar}.
     */
    public static final class Builder {
        private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        private @Nullable AgentClient agentClient;
        private int heartbeatPeriod = 1;
        private TimeUnit heartbeatPeriodTimeUnit = TimeUnit.MINUTES;
        private List<String> tags = new ArrayList<>();
        private Set<String> excludedServices = new HashSet<>();
        private List<Check> checks = new ArrayList<>();
        private @Nullable String consulToken = null;

        /**
         * @see #newBuilder()
         */
        private Builder() {}

        /* scheduledExecutorService */

        public ScheduledExecutorService getScheduledExecutorService() {
            return scheduledExecutorService;
        }

        public void setScheduledExecutorService(final ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = checkNotNull(scheduledExecutorService, "scheduledExecutorService");
        }

        public Builder withScheduledExecutorService(final ScheduledExecutorService scheduledExecutorService) {
            setScheduledExecutorService(scheduledExecutorService);
            return this;
        }

        /* agentClient */

        @Nullable
        public AgentClient getAgentClient() {
            return agentClient;
        }

        public void setAgentClient(final AgentClient agentClient) {
            this.agentClient = checkNotNull(agentClient, "agentClient");
        }

        public Builder withAgentClient(final AgentClient agentClient) {
            setAgentClient(agentClient);
            return this;
        }

        /* heartbeatPeriod */

        public int getHeartbeatPeriod() {
            return heartbeatPeriod;
        }

        public void setHeartbeatPeriod(final int heartbeatPeriod) {
            this.heartbeatPeriod = heartbeatPeriod;
        }

        public Builder withHeartbeatPeriod(final int heartbeatPeriod) {
            setHeartbeatPeriod(heartbeatPeriod);
            return this;
        }

        /* heartbeatPeriodTimeUnit */

        public TimeUnit getHeartbeatPeriodTimeUnit() {
            return heartbeatPeriodTimeUnit;
        }

        public void setHeartbeatPeriodTimeUnit(final TimeUnit heartbeatPeriodTimeUnit) {
            this.heartbeatPeriodTimeUnit = checkNotNull(heartbeatPeriodTimeUnit, "heartbeatPeriodTimeUnit");
        }

        public Builder withHeartbeatPeriodTimeUnit(final TimeUnit heartbeatPeriodTimeUnit) {
            setHeartbeatPeriodTimeUnit(heartbeatPeriodTimeUnit);
            return this;
        }

        /* tags */

        public List<String> getTags() {
            return tags;
        }

        public void setTags(final List<String> tags) {
            this.tags = checkNotNull(tags, "tags");
        }

        public Builder withTags(final List<String> tags) {
            setTags(tags);
            return this;
        }

        public Builder withTag(final String tag) {
            tags.add(checkNotNull(Strings.emptyToNull(tag), "tag"));
            return this;
        }

        /* excludedServices */

        public Set<String> getExcludedServices() {
            return excludedServices;
        }

        public void setExcludedServices(final Set<String> excludedServices) {
            this.excludedServices = checkNotNull(excludedServices, "excludedServices");
        }

        public Builder withExcludedServices(final Set<String> excludedServices) {
            setExcludedServices(excludedServices);
            return this;
        }

        public Builder withExcludedService(final String excludedService) {
            excludedServices.add(checkNotNull(Strings.emptyToNull(excludedService), "excludedService"));
            return this;
        }

        /* checks */

        public List<Check> getChecks() {
            return checks;
        }

        public void setChecks(final List<Check> checks) {
            this.checks = checkNotNull(checks, "checks");
        }

        public Builder withChecks(final List<Check> checks) {
            setChecks(checks);
            return this;
        }

        public Builder withCheck(final Check check) {
            checks.add(checkNotNull(check, "check"));
            return this;
        }

        /* consul token */
        public void setConsulToken(final String consulToken) {
            this.consulToken = checkNotNull(consulToken, "consulToken");
        }

        public Builder withConsulToken(final String consulToken) {
            setConsulToken(consulToken);
            return this;
        }

        /* build */

        public ConsulServiceRegistrar build() {
            return new ConsulServiceRegistrar(
                    checkNotNull(scheduledExecutorService, "scheduledExecutorService"),
                    checkNotNull(agentClient, "agentClient"),
                    heartbeatPeriod,
                    heartbeatPeriodTimeUnit,
                    checkNotNull(tags, "tags"),
                    checkNotNull(excludedServices, "excludedServices"),
                    checkNotNull(checks, "checks"),
                    consulToken
            );
        }
    }
}
