package org.gzc.kafka.repository;

import lombok.Data;
import lombok.ToString;

import java.util.List;

@Data
@ToString
public class LiveMessage {
    private int partition;
    private long beginningOffset;
    private long endOffset;
    private List<ConsumerMessage> messages;
}
