package microservices.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Simple class holding necessary data for transmitting a message from a party to another.
 * It contains the sender, the target, the type and the content of the message.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Message {

    private String sender;

    private String target;

    private Map<Object, Object> content;

    private MessageType type;

    public enum MessageType {
        CONNECT, HEARTBEAT, INFO, DISCONNECT
    }

}