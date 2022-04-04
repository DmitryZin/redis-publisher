package ru.spbe.redispublisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import ru.spbe.redisstarter.Redis;
import ru.spbe.redisstarter.RedisSet;

import java.util.*;

/**
 * Публикация данных в Redis. Имеет возможность публиковать как просто справочник. так и key\value справочник
 * @param <T> Тип публикуемых данных
 *
 */

@RequiredArgsConstructor
public class RedisPublisher<T> {

    /**
     * хост сервера Redis
     */
    private final String host;
    /**
     * порт сервера Redis
     */
    private final int port;
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
        this.host = host;
        this.port = port;
        this.set = set;
        this.mapper = mapper;

        redis = new Redis(host, port);
    }

    private void publishMessage() {
        if ((set.getChannelName() != null) && (!set.getChannelName().isEmpty())) {
            redis.publishMessage(set.getChannelName(), "PING");
        }
    }

    public void publish(T value) throws JsonProcessingException {
        redis.addValueToDataSet(set.getDataSetName(), mapper.writeValueAsString(value));
        publishMessage();
    }

    public void publish(String key, T value) throws JsonProcessingException {
        redis.addValueToKeyDataSet(set.getDataSetName(), key, mapper.writeValueAsString(value));
        publishMessage();
    }

    public void publish(Map<String, T> value) throws JsonProcessingException {
        Map<String, String> keys = new HashMap<>();
        for (Map.Entry<String, T> map : value.entrySet()) {
            keys.put(map.getKey(),  mapper.writeValueAsString(map.getValue()));
        }
        redis.addValuesToKeyDataSet(set.getDataSetName(), keys);
        publishMessage();
    }

    public void publish(List<T> values) throws JsonProcessingException{
        Set<String> setValues = new HashSet<>();
        for(T t : values){
            setValues.add(mapper.writeValueAsString(t));
        }
        redis.addValuesToDataSet(set.getDataSetName(), setValues);
        publishMessage();
    }

    public void clearValues(){
        redis.clearDataSet(set.getDataSetName());
    }
}
