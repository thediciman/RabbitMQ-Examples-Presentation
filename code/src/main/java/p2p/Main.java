package p2p;

import lombok.SneakyThrows;
import p2p.model.Peer;

import java.util.Scanner;

public class Main {

    /**
     * URI of the remote RabbitMQ server, so we don't have to run it locally.
     * If you do, however, wish to run locally, you can install RabbitMQ from https://www.rabbitmq.com/download.html
     * <p>
     * Anyone can sign up really easily and get access to such a server at https://www.cloudamqp.com
     * <p>
     * All you have to do is log in, create a new Little Lemur instance (which is free), select the region
     * which is preferably closest to you. After you create the instance, click on it, copy the AMQP URL from the
     * dashboard and paste it in this constant, and you're good to go!
     */
    private final static String SERVER_URI = "amqps://xmipxhvv:QStFl3F925vrL1IDyhl5GZfrrnZorb7n@chinook.rmq.cloudamqp.com/xmipxhvv";

    /**
     * Entry point in the application.
     * <p>
     * It prompts the user for its username, sets up a connection to the RabbitMQ server, the peer starts
     * listening to messages and the user can start chatting.
     */
    @SneakyThrows
    public static void main(final String[] args) {
        final Scanner scanner = new Scanner(System.in);

        System.out.print("Your username: ");
        final String username = scanner.nextLine();

        final Peer peer = new Peer(username, SERVER_URI);

        peer.initializeConnection();
        peer.listenToMessages();

        while (scanner.hasNext()) {
            peer.executeCommand(scanner.nextLine());
        }
    }

}