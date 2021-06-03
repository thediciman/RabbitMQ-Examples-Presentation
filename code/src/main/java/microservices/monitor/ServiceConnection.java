package microservices.monitor;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Class containing information related to a connection from a service to the monitor.
 */
@Data
@AllArgsConstructor
public class ServiceConnection {

    private final String serviceName;

    private ConnectionState connectionState;

    private Long heartbeatIntervalMilliseconds;

    private Long lastHeartbeatTimestamp;

    public enum ConnectionState {
        CONNECTED, TIMEOUT_ERROR, DISCONNECTED
    }

}