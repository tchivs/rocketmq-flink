package org.apache.flink.connector.rocketmq.sink.writer.serializer;

import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.common.message.Message;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocketMQEasySerializationSchema<T> implements RocketMQSerializationSchema<T> {

    protected final ObjectMapper objectMapper;
    private final String topic;
    private final String tags;
    private final Map<String, Method> methodCache;

    public RocketMQEasySerializationSchema(String topic, String tags) {
        this.topic = topic;
        this.tags = tags;
        this.objectMapper = new ObjectMapper();
        this.methodCache = new ConcurrentHashMap<>();
    }

    @Override
    public Message serialize(T element, RocketMQSinkContext context, Long timestamp) {
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(element);
            String key = extractMessageKey(element);
            String tag = extractMessageTag(element);
            if (tag == null) {
                tag = this.tags;
            }
            if (key != null) {
                return new Message(topic, tag, key, bytes);
            }
            return new Message(topic, tag, bytes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize element: " + element, e);
        }
    }

    private String extractField(T element, String fieldName) {
        try {
            Method getKeyMethod =
                    methodCache.computeIfAbsent(
                            element.getClass().getName() + "_" + fieldName,
                            clazz -> {
                                try {

                                    return element.getClass().getMethod("get" + fieldName);
                                } catch (NoSuchMethodException e) {
                                    return null;
                                }
                            });

            if (getKeyMethod != null) {
                Object key = getKeyMethod.invoke(element);
                return key != null ? key.toString() : null;
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract message key from element: " + element, e);
        }
    }

    private String extractMessageKey(T element) {
        return this.extractField(element, "Key");
    }

    private String extractMessageTag(T element) {
        return this.extractField(element, "Tag");
    }
}
