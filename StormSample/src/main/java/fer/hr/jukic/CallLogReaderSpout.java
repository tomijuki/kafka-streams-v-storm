package fer.hr.jukic;


import fer.hr.jukic.utils.LogUtil;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class CallLogReaderSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    private Map<Integer, Values> toSend = new ConcurrentHashMap<>();
    private Map<Integer, Values> allCalls = new ConcurrentHashMap<>();
    private Map<Integer, Integer> failCalls = new ConcurrentHashMap<>();
    private static final Integer MAX_RETRY = 5;
    private boolean completed = false;
    private TopologyContext context;
    private Random randomGenerator = new Random();
    private Integer idx = 0;

    public CallLogReaderSpout(){
        super();
        LogUtil.write2Log(this, "call CallLogReaderSpout constructor");
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.context = topologyContext;
        this.collector = spoutOutputCollector;
        LogUtil.write2Log(this, "call CallLogReaderSpout open");

        List<String> phoneNumbers = new ArrayList<String>();
        phoneNumbers.add("0989054575");
        phoneNumbers.add("0919054555");
        phoneNumbers.add("0999054777");
        phoneNumbers.add("0979054757");

        Integer localIdx = 0;
        for(int i = 0; i < 1000; i++) {
            String fromNumber = phoneNumbers.get(randomGenerator.nextInt(4));
            String toNumber = phoneNumbers.get(randomGenerator.nextInt(4));

            while (fromNumber == toNumber) {
                toNumber = phoneNumbers.get(randomGenerator.nextInt(4));
            }

            Integer duration = randomGenerator.nextInt(120);
            toSend.put(i, new Values(fromNumber, toNumber, duration));
        }
    }

    @Override
    public void nextTuple() {
        if(toSend!=null && !toSend.isEmpty()){
            for(Map.Entry<Integer,Values> entry : toSend.entrySet()){
                collector.emit(entry.getValue());
            }
            toSend.clear();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("from", "to", "duration"));
        LogUtil.write2Log(this, "call declareOutputFields in spout");
    }

    @Override
    public void close() {
        LogUtil.write2Log(this, "call close in spout");
    }
    public boolean isDistributed(){
        return false;
    }

    @Override
    public void activate() {}

    @Override
    public void deactivate() {}

    @Override
    public void ack(Object o) {
        LogUtil.write2Log(this, "call ack, object=" + o.toString());
        Integer mid = (Integer) o;
        toSend.remove(mid);
        failCalls.remove(mid);
    }

    @Override
    public void fail(Object o) {
        LogUtil.write2Log(this, "call fail, object=" + o.toString());
        Integer mid = (Integer) o;
        Integer count = failCalls.get(mid);
        count = count==null ? 0:count;
        if(count < MAX_RETRY){
            failCalls.put(mid, ++count);
            toSend.put(mid, allCalls.get(mid));
        } else {
            LogUtil.write2Log(this, "Maximum number of retries has been exceeded, call index = " + mid);
            toSend.remove(mid);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}