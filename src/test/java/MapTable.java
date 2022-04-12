import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class MapTable extends AbstractTable implements FilterableTable {
    private final RelDataType rowType;
    private final List<Object[]> data;

    public MapTable(RelDataType rowType, List<Object[]> data) {
        this.rowType = rowType;
        this.data = data;
    }


    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        final Pair<Integer, Object> filter = getEqualsFilter(filters);
        if (null != filter && filter.left != null && filter.left == 0) {

            List<Object[]> filtered = data.stream().filter(
                    book -> book[0] == filter.right
            ).collect(Collectors.toList());

                    System.out.printf("returning %d books (id = %d) (because of push-down!)%n", filtered.size(), filter.right);
            return Linq4j.asEnumerable(filtered);
        }

        return Linq4j.asEnumerable(data);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType;
    }

    private static Pair<Integer, Object> getEqualsFilter(List<RexNode> filters) {
        final Iterator<RexNode> filterIter = filters.iterator();
        while (filterIter.hasNext()) {
            final RexNode node = filterIter.next();
            if (node instanceof RexCall
                    && ((RexCall) node).getOperator() == SqlStdOperatorTable.EQUALS
                    && ((RexCall) node).getOperands().get(0) instanceof RexInputRef
                    && ((RexCall) node).getOperands().get(1) instanceof RexLiteral) {
                filterIter.remove();
                final int pos = ((RexInputRef) ((RexCall) node).getOperands().get(0)).getIndex();
                final RexLiteral op1 = (RexLiteral) ((RexCall) node).getOperands().get(1);
                switch (pos) {
                    case 0:
                    case 2:
                        return Pair.of(pos, ((BigDecimal) op1.getValue()).intValue());
                    case 1:
                        return Pair.of(pos, ((NlsString) op1.getValue()).getValue());
                }
            }
        }
        return null;


    }
}
