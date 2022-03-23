package ru.spbe.redispublisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import ru.spbe.redisstarter.IRedisSet;
import ru.spbe.redisstarter.Redis;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RequiredArgsConstructor
public class RedisPublisher<T> {
    private final Redis redis;
    private IRedisSet set;
    private final ObjectMapper mapper;

    public void addValue(T value) throws JsonProcessingException {
        String valueString = mapper.writeValueAsString(value);
        redis.addValue(set.getDataSetName(), valueString);
        if((set.getChannelName() != null) && (!set.getChannelName().isEmpty())){
            redis.publishMessage(set.getChannelName(), "addValue");
        }
    }

    public void addValues(List<T> values) throws JsonProcessingException{
        Set<String> setValues = new HashSet<>();
        for(T t : values){
            String valueString = mapper.writeValueAsString(t);
            setValues.add(valueString);
        }
        redis.addValues(set.getDataSetName(), setValues);
        if((set.getChannelName() != null) && (!set.getChannelName().isEmpty())){
            redis.publishMessage(set.getChannelName(), "addValues");
        }
    }

    public void clearValues(){
        redis.clearDataSet(set.getDataSetName());
    }
}
