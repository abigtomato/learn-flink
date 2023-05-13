package org.abigtomato.learn.example.windcontrol;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author abigtomato
 */
@Data
@AllArgsConstructor
public class OrderEvent {

    private Long orderId;
    private String eventType;
    private String txId;
    private Long timestamp;
}
