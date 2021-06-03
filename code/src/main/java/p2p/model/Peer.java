package p2p.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.Data;
import lombok.SneakyThrows;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Class representing the "peer" in the chat.
 * <p>
 * It handles the logic for connecting to the messaging server, sending and receiving messages from the connected peers.
 */
@Data
public class Peer {

    /**
     * The username of the peer.
     */
    private String username;

    /**
     * The URI of the server to which the peer wishes to connect to.
     */
    private String serverUri;

    /**
     * The connection and channel objects used to handle the message transfer.
     */
    private Connection connection;
    private Channel channel;

    /**
     * Object mapper used to convert Java objects into JSON and vice-versa.
     */
    private ObjectMapper objectMapper;

    /**
     * A map containing the state of the connection for a given peer.
     */
    private Map<String, ConnectionState> connectionsStates;

    /**
     * Instantiates the Peer object with the given username and server URI.
     */
    @SneakyThrows
    public Peer(final String username, final String serverUri) {
        this.username = username;
        this.serverUri = serverUri;
        this.objectMapper = new ObjectMapper();
        this.connectionsStates = new HashMap<>();
    }

    /**
     * Initializes the connection to the messaging server, creates a message queue with the name of the user in which
     * the user will receive messages and purges that queue, so there are no previous messages left prior to the current
     * session.
     */
    @SneakyThrows
    public void initializeConnection() {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(serverUri);
        connectionFactory.setConnectionTimeout(5000);

        connection = connectionFactory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(username, false, false, false, null);
        channel.queuePurge(username);

        System.out.printf("%nHello, %s. You are now logged in!%n%n", username);
    }

    /**
     * Destroys the connection to the messaging server and the associated channel.
     */
    @SneakyThrows
    public void destroyConnection() {
        channel.close();
        connection.close();
    }

    /**
     * Creates the callback which handles the received messages and starts consuming messages from the queue.
     */
    @SneakyThrows
    public void listenToMessages() {
        /*
          Define the DeliverCallback which handles the received messages.
         */
        final DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                /*
                    Get the message from the body of the delivery and convert it from JSON back to the Message class.
                 */
                final Message message = objectMapper.readValue(new String(delivery.getBody(), StandardCharsets.UTF_8), Message.class);

                /*
                    Extract the sender and content from the message to avoid further unnecessary method calls.
                 */
                final String sender = message.getSender();
                final String content = message.getContent();

                /*
                    If the received message is "!hello", a new connection will be established with the sender,
                    only if there is no connection in any state already with that specific sender.

                    The new connection's state will be TARGET_AWAITING_ACK, since the sender is now awaiting
                    an acknowledgement from the current peer.
                 */
                if ("!hello".equals(content)) {
                    if (!connectionsStates.containsKey(sender)) {
                        System.out.printf("%s wants to connect with you!%n", sender);
                        connectionsStates.put(sender, ConnectionState.TARGET_AWAITING_ACK);
                    }
                }

                /*
                    If the received message is "!ack", the state of the connection with the sender will be
                    updated to ACCEPTED, only if there already exists a connection with the sender and its state is
                    SELF_AWAITING_ACK, meaning the current peer was waiting for an acknowledgement from the sender.
                 */
                else if ("!ack".equals(content)) {
                    if (connectionsStates.containsKey(sender) && connectionsStates.get(sender).equals(ConnectionState.SELF_AWAITING_ACK)) {
                        connectionsStates.put(sender, ConnectionState.ACCEPTED);
                        System.out.printf("You have connected with %s!%n", sender);
                    }
                }

                /*
                    In all other cases, the message is a normal one and it will be printed to the peer, only if the
                    peer has accepted a connection with the sender.
                 */
                else {
                    if (connectionsStates.containsKey(sender) && connectionsStates.get(sender).equals(ConnectionState.ACCEPTED)) {
                        System.out.printf("[%s]: %s%n", sender, content);
                    }
                }
            } catch (final Exception exception) {
                /*
                    If an exception is encountered, it shall be printed.
                 */
                System.out.println("[ERROR]: " + exception);
            }
        };

        channel.basicConsume(username, true, deliverCallback, (consumerTag -> {
        }));
    }

    /**
     * Send a JSON containing a message to the embedded target.
     */
    @SneakyThrows
    private void sendMessage(final Message message) {
        channel.basicPublish("", message.getTarget(), null, objectMapper.writeValueAsBytes(message));
    }

    /**
     * Execute a command given from the user.
     * <p>
     * A command is in the form < !hello {username} | !ack {username} | !sendto {username} {content} | !exit >
     */
    @SneakyThrows
    public void executeCommand(final String command) {
        /*
            Split the command by whitespaces; the first token will always be the action.
         */
        final String[] commandTokens = command.split("\\s+", 2);
        final String action = commandTokens[0];

        /*
            If the action is "!exit", then the connection is destroyed and the execution of the program is terminated.
         */
        if ("!exit".equals(action)) {
            destroyConnection();
            System.exit(0);
        }

        /*
            If the action is "!hello", a new connection will be established with the target,
            only if there is no connection in any state already with that specific target.

            The new connection's state will be SELF_AWAITING_ACK, since the current peer is now awaiting
            an acknowledgement from the target.

            A message will also be sent to the target to notify them about the new connection from the current peer.
         */
        else if ("!hello".equals(action)) {
            final String target = commandTokens[1];

            if (!connectionsStates.containsKey(target)) {
                sendMessage(new Message(username, target, action));
                connectionsStates.put(target, ConnectionState.SELF_AWAITING_ACK);
            }
        }

        /*
            If the action is "!ack", the state of the connection with the target will be
            updated to ACCEPTED, only if there already exists a connection with the target and its state is
            TARGET_AWAITING_ACK, meaning the target was waiting for an acknowledgement from current peer.

            A message will also be sent to the target to notify them about the acknowledgement from the current peer.
         */
        else if ("!ack".equals(action)) {
            final String target = commandTokens[1];

            if (connectionsStates.containsKey(target) && connectionsStates.get(target).equals(ConnectionState.TARGET_AWAITING_ACK)) {
                sendMessage(new Message(username, target, action));
                connectionsStates.put(target, ConnectionState.ACCEPTED);
                System.out.printf("You have connected with %s!%n", target);
            }
        }

        /*
            If the action is "!sendto", a message will be sent to the target only if the target
            has accepted a connection with the current peer.
         */
        else if ("!sendto".equals(action)) {
            final String[] targetAndContent = commandTokens[1].split("\\s+", 2);
            final String target = targetAndContent[0];

            if (connectionsStates.containsKey(target) && connectionsStates.get(target).equals(ConnectionState.ACCEPTED)) {
                sendMessage(new Message(username, target, targetAndContent[1]));
            }
        }

        /*
            If the action doesn't match with any of the previous cases, then it is invalid.
         */
        else {
            System.out.println("Please, enter a valid command.");
        }
    }

    /**
     * Enum representing the possible states of a connection with a peer.
     */
    public enum ConnectionState {

        /**
         * The current peer is waiting for an acknowledgment message from a counterparty.
         */
        SELF_AWAITING_ACK,

        /**
         * The counterparty is waiting for an acknowledgment message from the current peer.
         */
        TARGET_AWAITING_ACK,

        /**
         * The connection has been accepted and acknowledged by both parties
         * and they can now transfer messages between themselves.
         */
        ACCEPTED

    }

}