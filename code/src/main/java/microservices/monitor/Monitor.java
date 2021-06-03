package microservices.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.SneakyThrows;
import microservices.model.Message;
import microservices.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static microservices.utils.Utils.logMessageWithTimestamp;

public class Monitor {

    /**
     * The name of the queue on which the monitor will receive messages.
     */
    public final static String MONITOR_QUEUE_NAME = "monitor";

    /**
     * The URI of the server to which the peer wishes to connect to.
     */
    private final String serverUri;

    /**
     * Object mapper used to convert Java objects into JSON and vice-versa.
     */
    private final ObjectMapper objectMapper;

    /**
     * Map in which can be found all the services which were running
     * or are still running, along with their state.
     */
    private final Map<String, ServiceConnection> serviceConnections;

    /**
     * Scheduled thread pool executor used for monitoring the heartbeats
     * received from the services.
     */
    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    /**
     * The connection and channel objects used to handle the message transfer.
     */
    private Connection connection;
    private Channel channel;

    /**
     * Instantiates the Monitor object with the given server URI
     * and initializes internal state variables.
     */
    public Monitor(final String serverUri) {
        this.serverUri = serverUri;
        this.objectMapper = new ObjectMapper();
        this.serviceConnections = new HashMap<>();
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
    }

    /**
     * Entry point for the monitor application.
     */
    @SneakyThrows
    public static void main(final String[] args) {
        final Monitor monitor = new Monitor(Utils.SERVER_URI);
        monitor.initializeConnection();
        monitor.listenToMessages();
        monitor.startHeartbeatMonitoring();

        final Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            final String line = scanner.nextLine();

            if (line.equals("exit")) {
                break;
            } else if (line.equals("check")) {
                monitor.printServicesState();
            }
        }

        monitor.destroyConnection();
    }

    /**
     * Prints the state of each monitored service.
     */
    private void printServicesState() {
        if (serviceConnections.size() == 0) {
            logMessageWithTimestamp("There are no monitored services yet.");
        } else {
            serviceConnections
                .forEach(
                    (serviceName, serviceConnection) -> logMessageWithTimestamp("Service %s: %s", serviceName, serviceConnection.getConnectionState())
                );
        }
    }

    /**
     * Initializes the connection to the messaging server and creates a message queue
     * in which the monitor will receive messages from the services.
     */
    @SneakyThrows
    public void initializeConnection() {
        logMessageWithTimestamp("Initializing monitor connections.");

        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(serverUri);
        connectionFactory.setConnectionTimeout(5000);

        connection = connectionFactory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(MONITOR_QUEUE_NAME, false, false, false, null);
        channel.queuePurge(MONITOR_QUEUE_NAME);

        logMessageWithTimestamp("Monitor is running!");
    }

    /**
     * Destroys the connection to the messaging server and the associated channel,
     * and the executor service used for monitoring heartbeats.
     */
    @SneakyThrows
    public void destroyConnection() {
        logMessageWithTimestamp("Destroying connections and closing the monitor.");

        scheduledThreadPoolExecutor.shutdown();
        channel.close();
        connection.close();
    }

    /**
     * Creates the callback which handles the received messages and starts consuming messages from the queue.
     */
    @SneakyThrows
    public void listenToMessages() {
        final DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                final Message message = objectMapper.readValue(new String(delivery.getBody(), StandardCharsets.UTF_8), Message.class);
                final String sender = message.getSender();

                final Message.MessageType messageType = message.getType();
                switch (messageType) {
                    case CONNECT -> {
                        final Long heartbeatIntervalMillis = Long.valueOf(String.valueOf(message.getContent().get(Utils.HEARTBEAT_INTERVAL_PROPERTY)));
                        logMessageWithTimestamp("Service %s has connected, heartbeat interval is %d millis!", sender, heartbeatIntervalMillis);
                        serviceConnections.put(sender, new ServiceConnection(sender, ServiceConnection.ConnectionState.CONNECTED, heartbeatIntervalMillis, System.currentTimeMillis()));
                    }
                    case HEARTBEAT -> {
                        logMessageWithTimestamp("Received heartbeat from service %s!", sender);
                        final ServiceConnection serviceConnection = serviceConnections.get(sender);
                        serviceConnection.setLastHeartbeatTimestamp(System.currentTimeMillis());
                        serviceConnection.setConnectionState(ServiceConnection.ConnectionState.CONNECTED);
                    }
                    case INFO -> logMessageWithTimestamp(
                        "Received an information message from service %s: %s",
                        sender,
                        message.getContent().get(Utils.INFO_PROPERTY).toString()
                    );
                    case DISCONNECT -> {
                        logMessageWithTimestamp("Service %s has disconnected!", sender);
                        serviceConnections.get(sender).setConnectionState(ServiceConnection.ConnectionState.DISCONNECTED);
                    }
                }
            } catch (final Exception exception) {
                logMessageWithTimestamp("[ERROR]: " + exception);
            }
        };

        channel.basicConsume(MONITOR_QUEUE_NAME, true, deliverCallback, (consumerTag -> {
        }));
    }

    /**
     * Starts a thread that checks at a given time interval
     * if the services have sent a heartbeat and updates their states accordingly.
     */
    public void startHeartbeatMonitoring() {
        logMessageWithTimestamp("Started monitoring the heartbeats.");

        scheduledThreadPoolExecutor.scheduleWithFixedDelay(
            () -> {
                logMessageWithTimestamp("Checking heartbeats...");
                serviceConnections
                    .forEach((serviceName, serviceConnection) -> {
                        if (serviceConnection.getConnectionState().equals(ServiceConnection.ConnectionState.CONNECTED)) {
                            final long currentTimeMillis = System.currentTimeMillis();
                            final long lastHeartbeatTimestamp = serviceConnection.getLastHeartbeatTimestamp();

                            if (currentTimeMillis - lastHeartbeatTimestamp > serviceConnection.getHeartbeatIntervalMilliseconds()) {
                                serviceConnection.setConnectionState(ServiceConnection.ConnectionState.TIMEOUT_ERROR);
                                logMessageWithTimestamp("Service %s did not send a heartbeat in the specified time interval, setting state as timeout error.", serviceName);
                            }
                        }
                    });
            },
            0L,
            5000,
            TimeUnit.MILLISECONDS
        );
    }

}