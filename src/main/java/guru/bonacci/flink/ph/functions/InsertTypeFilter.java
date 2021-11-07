package guru.bonacci.flink.ph.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class InsertTypeFilter implements FilterFunction<Row> {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean filter(Row row) throws Exception {
		return RowKind.INSERT.equals(row.getKind());
	}
}
