package org.abigtomato.learn;

import lombok.*;

/**
 * 传感器读数实体类
 *
 * @author abigtomato
 */
@Data
@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {

    private String id;

    /**
     * 时间戳
     */
    private Long timestamp;

    /**
     * 温度
     */
    private Double temperature;
}
