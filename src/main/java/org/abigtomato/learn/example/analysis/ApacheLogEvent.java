package org.abigtomato.learn.example.analysis;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author abigtomato
 */
@Data
@AllArgsConstructor
public class ApacheLogEvent {

    private String ip;
    private String userId;
    private Long timestamp;
    private String method;
    private String url;
}
