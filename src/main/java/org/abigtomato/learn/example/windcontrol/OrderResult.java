package org.abigtomato.learn.example.windcontrol;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author abigtomato
 */
@Data
@AllArgsConstructor
public class OrderResult {

    private Long orderId;
    private String resultState;
}
