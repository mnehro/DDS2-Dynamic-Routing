package org.dds.messages;

import java.util.Map;

public record PutAllMessage(Map<Integer, Double> newData) {
}
