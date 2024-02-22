package fer.hr.jukic;

import fer.hr.jukic.utils.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class CallLogCounterBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private Map<String, Integer> counterMap;
    private OutputCollector collector;

    public CallLogCounterBolt(){
        LogUtil.write2Log(this, "call CallLogCounterBolt constructor");
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counterMap = new HashMap<String, Integer>();
        this.collector = outputCollector;
        LogUtil.write2Log(this, "call prepare in counter bolt");
    }

    @Override
    public void execute(Tuple tuple) {
        String call = tuple.getString(0);
        Integer duration = tuple.getInteger(1);
        if(new Random().nextBoolean()){
            if(!counterMap.containsKey(call)){
                counterMap.put(call,1);
            } else {
                Integer c = counterMap.get(call) + 1;
                counterMap.put(call, c);
            }
            collector.ack(tuple);
            LogUtil.write2Log(this,call+":ack");
        } else {
            collector.fail(tuple);
            LogUtil.write2Log(this, call+":fail");
        }
    }

    @Override
    public void cleanup() {
        LogUtil.write2Log(this, "call counter bolt cleanup");
        for(Map.Entry<String, Integer> entry: counterMap.entrySet()){
            LogUtil.write2Log(this, entry.getKey() + " : " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
