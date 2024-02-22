package fer.hr.jukic;

import fer.hr.jukic.utils.LogUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class CallLogCreatorBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    public CallLogCreatorBolt(){
        super();
        LogUtil.write2Log(this, "call CallLogCreatorBolt constructor");
        /*try {
            Thread.sleep(10);
        } catch (Exception e) {}*/
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        LogUtil.write2Log(this, "call prepare in creator bolt");
    }

    @Override
    public void execute(Tuple tuple) {
        String from = tuple.getString(0);
        String to = tuple.getString(1);
        Integer duration = tuple.getInteger(2);
        collector.emit(new Values(from + " - " + to, duration));
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        LogUtil.write2Log(this, "call creator cleanup");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("call", "duration"));
        LogUtil.write2Log(this, "call declareOutputFields in creator bolt");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
