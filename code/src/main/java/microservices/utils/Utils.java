package microservices.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {

    public static final String HEARTBEAT_INTERVAL_PROPERTY = "hbInterval";

    public static final String INFO_PROPERTY = "info";

    /**
     * URI of the remote RabbitMQ server, so we don't have to run it locally.
     * <p>
     * Anyone can sign up really easily and get access to such a server at https://www.cloudamqp.com
     * <p>
     * All you have to do is log in, create a new Little Lemur instance (which is free), select the region
     * which is preferably closest to you. After you create the instance, click on it, copy the AMQP URL from the
     * dashboard and paste it in this constant, and you're good to go!
     */
    public final static String SERVER_URI = "amqps://xmipxhvv:QStFl3F925vrL1IDyhl5GZfrrnZorb7n@chinook.rmq.cloudamqp.com/xmipxhvv";

    /**
     * Utility method used to log a message along with the current time.
     */
    public static void logMessageWithTimestamp(final String message) {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        System.out.printf("[%s]: %s%n", simpleDateFormat.format(new Date()), message);
    }

    /**
     * Utility method used to log a message in a given format along with the current time.
     */
    public static void logMessageWithTimestamp(final String format, final Object... args) {
        logMessageWithTimestamp(String.format(format, args));
    }

}