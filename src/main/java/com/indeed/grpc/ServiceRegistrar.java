package com.indeed.grpc;

/**
 * An interface that defines how to announce to clients that a new instance is available.
 *
 * @author pitz@indeed.com (Jaye Pitzeruse)
 */
public interface ServiceRegistrar {
    /**
     * Registers a single service into consul.
     *
     * @param serviceName The name of the service.
     * @param advertiseAddress The address the service is listening to.
     * @param port The port that the service is listening on.
     */
    void registerService(final String serviceName, final String advertiseAddress, final int port);
}
