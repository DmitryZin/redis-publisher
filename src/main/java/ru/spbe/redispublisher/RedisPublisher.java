package ru.spbe.redispublisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import ru.spbe.redisstarter.Redis;
import ru.spbe.redisstarter.RedisSet;

import java.util.*;

@RequiredArgsConstructor
public class RedisPublisher<K, V> {
    /**
     * справочник
     */
    private final RedisSet set;
    /**
     * маппер для сериализации
     */
    private final ObjectMapper mapper;

    private final Redis redis;

    public RedisPublisher(String host, int port, RedisSet set, ObjectMapper mapper) {
        this.set = set;
        this.mapper = mapper;

        redis = new Redis(host, port);
    }

    private void publishMessage() {
        if ((set.getChannelName() != null) && (!set.getChannelName().isEmpty())) {
            redis.publishMessage(set.getChannelName(), "PING");
        }
    }

    public void publish(K key, V value) throws JsonProcessingException {
        redis.addValueToKeyDataSet(set.getDataSetName(), mapper.writeValueAsString(key), mapper.writeValueAsString(value));
        publishMessage();
    }

    public void publish(Map<K, V> value) throws JsonProcessingException {
        Map<String, String> keys = new HashMap<>();
        for (Map.Entry<K, V> map : value.entrySet()) {
            keys.put(mapper.writeValueAsString(map.getKey()),  mapper.writeValueAsString(map.getValue()));
        }
        redis.addValuesToKeyDataSet(set.getDataSetName(), keys);
        publishMessage();
    }

    public void clearValues(){
        redis.clearDataSet(set.getDataSetName());
    }
}
