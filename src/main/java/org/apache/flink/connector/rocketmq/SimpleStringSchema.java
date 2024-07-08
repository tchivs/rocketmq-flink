package org.apache.flink.connector.rocketmq;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.common.message.Message;

import java.io.IOException;

public class SimpleStringSchema
        implements RocketMQSerializationSchema<String>, RocketMQDeserializationSchema<String> {
    protected final ObjectMapper objectMapper;
    private final String topic;
    private final String tags;

    public SimpleStringSchema(String topic, String tags) {
        this.topic = topic;
        this.tags = tags;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void deserialize(MessageView messageView, Collector<String> out) throws IOException {
        out.collect(new String(messageView.getBody()));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    @Override
    public Message serialize(String element, RocketMQSinkContext context, Long timestamp) {

        return new Message(topic, tags, element.getBytes());
    }
}
