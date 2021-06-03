package microservices.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.Data;
import lombok.SneakyThrows;
import microservices.model.Message;
import microservices.monitor.Monitor;
import microservices.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static microservices.utils.Utils.logMessageWithTimestamp;

@Data
public class Service {

    /**
     * The name of the queue on which the service will receive messages.
     */
    private final String serviceName;

    /**
     * The interval at which the service sends a heartbeat to the monitor.
     */
    private final Long heartbeatInterval;

    /**
     * The URI of the server to which the peer wishes to connect to.
     */
    private final String serverUri;

    /**
     * Object mapper used to convert Java objects into JSON and vice-versa.
     */
    private final ObjectMapper objectMapper;

    /**
     * Scheduled thread pool executor used to send heartbeats to the monitor
     * at the specified heartbeat interval.
     */
    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    /**
     * The connection and channel objects used to handle the message transfer.
     */
    private Connection connection;
    private Channel channel;

    /**
     * Instantiates the Service object with the given server URI
     * and initializes internal state variables.
     */
    public Service(final String serviceName, final Long heartbeatInterval, final String serverUri) {
        this.serviceName = serviceName;
        this.heartbeatInterval = heartbeatInterval;
        this.serverUri = serverUri;
        this.objectMapper = new ObjectMapper();
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
    }

    /**
     * Entry point for the service application.
     */
    public static void main(final String[] args) {
        if (args.length != 2) {
            System.err.println("The service requires two program arguments: the service name and the heartbeat interval");
            System.exit(-1);
        }

        final Service service = new Service(args[0], Long.valueOf(args[1]), Utils.SERVER_URI);
        service.initializeConnection();
        service.connectToMonitor();
        service.startSendingHeartbeats();
        service.listenToMessages();

        final Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            final String line = scanner.nextLine();
            if (line.equals("exit")) {
                break;
            } else {
                final Map<Object, Object> messageContent = new HashMap<>();
                messageContent.put(Utils.INFO_PROPERTY, line);

                final Message message = Message
                    .builder()
                    .sender(service.getServiceName())
                    .target(Monitor.MONITOR_QUEUE_NAME)
                    .content(messageContent)
                    .type(Message.MessageType.INFO)
                    .build();

                service.sendMessage(message);
            }
        }

        service.disconnectFromMonitor();
        service.destroyConnection();
    }

    /**
     * Schedules the executor to send heartbeats to the monitor
     * at the specified interval.
     */
    private void startSendingHeartbeats() {
        logMessageWithTimestamp("Started heartbeat scheduler.");

        scheduledThreadPoolExecutor.scheduleWithFixedDelay(
            () -> {
                logMessageWithTimestamp("Sending heartbeat to monitor.");

                final Message message = Message
                    .builder()
                    .sender(serviceName)
                    .target(Monitor.MONITOR_QUEUE_NAME)
                    .type(Message.MessageType.HEARTBEAT)
                    .build();

                sendMessage(message);
            },
            0L,
            heartbeatInterval,
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Send a JSON containing a message to the embedded target.
     */
    @SneakyThrows
    public void sendMessage(final Message message) {
        channel.basicPublish("", message.getTarget(), null, objectMapper.writeValueAsBytes(message));
    }

    /**
     * Sends a message to the monitor stating that the service is now running and connected to the monitor.
     */
    private void connectToMonitor() {
        logMessageWithTimestamp("Sending CONNECT message to monitor.");

        final Map<Object, Object> messageContent = new HashMap<>();
        messageContent.put(Utils.HEARTBEAT_INTERVAL_PROPERTY, heartbeatInterval);

        final Message message = Message
            .builder()
            .sender(serviceName)
            .target(Monitor.MONITOR_QUEUE_NAME)
            .type(Message.MessageType.CONNECT)
            .content(messageContent)
            .build();

        sendMessage(message);
    }

    /**
     * Sends a message to the monitor stating that the service is shutting down and disconnecting from the monitor.
     */
    private void disconnectFromMonitor() {
        logMessageWithTimestamp("Sending DISCONNECT message to monitor.");

        final Message message = Message
            .builder()
            .sender(serviceName)
            .target(Monitor.MONITOR_QUEUE_NAME)
            .type(Message.MessageType.DISCONNECT)
            .build();

        sendMessage(message);
    }

    /**
     * Initializes the connection to the messaging server and creates a message queue
     * in which the service will receive messages from the services.
     */
    @SneakyThrows
    public void initializeConnection() {
        logMessageWithTimestamp("Initializing service connections.");

        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(serverUri);
        connectionFactory.setConnectionTimeout(5000);

        connection = connectionFactory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(serviceName, false, false, false, null);
        channel.queuePurge(serviceName);
    }

    /**
     * Creates the callback which handles the received messages and starts consuming messages from the queue.
     */
    @SneakyThrows
    public void listenToMessages() {
        final DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                final Message message = objectMapper.readValue(new String(delivery.getBody(), StandardCharsets.UTF_8), Message.class);
                logMessageWithTimestamp("Received message: %s", message);
            } catch (final Exception exception) {
                logMessageWithTimestamp("[ERROR]: " + exception);
            }
        };

        channel.basicConsume(serviceName, true, deliverCallback, (consumerTag -> {
        }));
    }

    /**
     * Destroys the connection to the messaging server and the associated channel.
     */
    @SneakyThrows
    public void destroyConnection() {
        logMessageWithTimestamp("Destroying connections and closing the monitor.");

        scheduledThreadPoolExecutor.shutdown();
        channel.close();
        connection.close();
    }

}