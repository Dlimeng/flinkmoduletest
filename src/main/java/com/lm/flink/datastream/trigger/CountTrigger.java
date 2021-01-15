package com.lm.flink.datastream.trigger;

import com.lm.flink.model.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Classname CountTrigger
 * @Description TODO
 * @Date 2021/1/12 19:32
 * @Created by limeng
 */
public class CountTrigger extends Trigger<FlinkSourceTrigger.model, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final long maxCount;

    private final  ReducingStateDescriptor<Long> stateDesc = new ReducingStateDescriptor<>("count",new Sum(), LongSerializer.INSTANCE);

    public CountTrigger(long maxCount) {
        super();
        this.maxCount = maxCount;
    }

    @Override
    public TriggerResult onElement(FlinkSourceTrigger.model element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

        ctx.registerProcessingTimeTimer(window.maxTimestamp());

        ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
        count.add(1L);
        if(count.get() >= maxCount){
            count.clear();
            return TriggerResult.FIRE_AND_PURGE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(stateDesc).clear();
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(stateDesc);
        long l = window.maxTimestamp();
        if(l > ctx.getCurrentProcessingTime()){
            ctx.registerProcessingTimeTimer(l);
        }
    }


    private static class  Sum implements ReduceFunction<Long>{
        private static final long serialVersionUID = 2L;
        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }
}
