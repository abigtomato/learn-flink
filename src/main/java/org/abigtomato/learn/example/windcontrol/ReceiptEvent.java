package org.abigtomato.learn.example.windcontrol;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author abigtomato
 */
@Data
@AllArgsConstructor
public class ReceiptEvent {

    private String txId;
    private String payChannel;
    private Long timestamp;
}
