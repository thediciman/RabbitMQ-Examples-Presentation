package p2p.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Simple class holding necessary data for transmitting a message from a party to another.
 * It contains the sender, the target and the content of the message.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {

    private String sender;

    private String target;

    private String content;

}