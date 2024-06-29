package org.apache.flink.connector.rocketmq.sink.writer.serializer;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;
import org.apache.flink.metrics.Counter;
import org.apache.rocketmq.common.message.Message;

public class RocketMQEasySerializationSchema<T> implements RocketMQSerializationSchema<T> {

    protected final ObjectMapper objectMapper;
    private final String topic;
    private final String tags;

    public RocketMQEasySerializationSchema(String topic, String tags) {
        this.topic = topic;
        this.tags = tags;
        this.objectMapper = new ObjectMapper();
    }



    @Override
    public Message serialize(T element, RocketMQSinkContext context, Long timestamp) {

        try {
            byte[] bytes = objectMapper.writeValueAsBytes(element);

            return new Message(topic, tags, bytes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize element: " + element, e);
        }
    }




}