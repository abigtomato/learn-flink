package org.abigtomato.learn.example.analysis;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author abigtomato
 */
@Data
@AllArgsConstructor
public class ItemViewCount {

    private Long itemId;
    private Long windowStart;
    private Long windowEnd;
    private Long count;
}
