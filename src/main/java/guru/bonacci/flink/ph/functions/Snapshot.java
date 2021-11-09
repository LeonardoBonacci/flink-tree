package guru.bonacci.flink.ph.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import guru.bonacci.flink.ph.model.HierarchyWrapper;

public class Snapshot extends RichMapFunction<HierarchyWrapper, HierarchyWrapper> {

	private static final long serialVersionUID = 1L;

	private transient ValueState<HierarchyWrapper> backup;	

    @Override
	public HierarchyWrapper map(HierarchyWrapper value) throws Exception {
    	backup.update(value);
        return backup.value();
    }

    @Override
    public void open(Configuration config) {
    	StateTtlConfig ttlConfig = StateTtlConfig
    		    .newBuilder(Time.days(1))
    		    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    		    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    		    .build();
    	
    	ValueStateDescriptor<HierarchyWrapper> stateDescriptor =
                new ValueStateDescriptor<>(
                        "hierarchy", 
                        TypeInformation.of(new TypeHint<HierarchyWrapper>() {}));
        stateDescriptor.enableTimeToLive(ttlConfig);
        backup = getRuntimeContext().getState(stateDescriptor);
    }
}
