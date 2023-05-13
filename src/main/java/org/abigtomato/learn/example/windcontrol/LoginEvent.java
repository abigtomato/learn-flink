package org.abigtomato.learn.example.windcontrol;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author abigtomato
 */
@Data
@AllArgsConstructor
public class LoginEvent {

    private Long userId;
    private String ip;
    private String loginState;
    private Long timestamp;
}
