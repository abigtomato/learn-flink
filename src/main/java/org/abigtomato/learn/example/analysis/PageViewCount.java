package org.abigtomato.learn.example.analysis;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author abigtomato
 */
@Data
@AllArgsConstructor
public class PageViewCount {

    private String url;
    private Long windowEnd;
    private Long count;
}
