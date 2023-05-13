package org.abigtomato.learn.example.windcontrol;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author abigtomato
 */
@Data
@AllArgsConstructor
public class LoginFailWarning {

    private Long userId;
    private Long firstFailTime;
    private Long lastFailTime;
    private String warningMsg;
}
