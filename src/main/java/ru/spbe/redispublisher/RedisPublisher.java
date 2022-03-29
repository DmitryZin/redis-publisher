package ru.spbe.redispublisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import ru.spbe.redisstarter.Redis;
import ru.spbe.redisstarter.RedisSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RequiredArgsConstructor
public class RedisPublisher<T> {
    private final Redis redis;
    private final RedisSet set;
    private final ObjectMapper mapper;

    private void publishMessage() {
        if ((set.getChannelName() != null) && (!set.getChannelName().isEmpty())) {
            redis.publishMessage(set.getChannelName(), "PING");
        }
    }

    public void addValue(T value) throws JsonProcessingException {
        redis.addValue(set.getDataSetName(), mapper.writeValueAsString(value));
        publishMessage();
    }

    public void addValues(List<T> values) throws JsonProcessingException{
        Set<String> setValues = new HashSet<>();
        for(T t : values){
            setValues.add(mapper.writeValueAsString(t));
        }
        redis.addValues(set.getDataSetName(), setValues);
        publishMessage();
    }

    public void clearValues(){
        redis.clearDataSet(set.getDataSetName());
    }
}
