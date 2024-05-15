package org.dds.messages;

public record ReplaceMessage(Integer key, Double newValue) implements Message {
}
