import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class DemoQueryProcessing {
    public static final List<Object[]> BOOK_DATA = Arrays.asList(
            // columns are:
            // * id
            // * title
            // * publish_year
            // * author
            new Object[]{1, "Les Miserables", 1862, "Victor Hugo"},
            new Object[]{2, "The Hunchback of Notre-Dame", 1829, "Victor Hugo"},
            new Object[]{3, "The Last Day of a Condemned Man", 1829, "Victor Hugo"},
            new Object[]{4, "The three Musketeers", 1844, "Alexandre Dumas"},
            new Object[]{5, "The Count of Monte Cristo", 1884, "Alexandre Dumas"}
    );

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
            , viewPath) -> null;

    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelOptCluster cluster = newCluster(typeFactory);

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    @Test
    public void DemoLogicalPlan() throws SqlParseException {
        CalciteSchema schema = CalciteSchema.createRootSchema(true);

        // create our fake Table
        Table bookTable = createBookTable();
        schema.add("books", bookTable);

        // Create an SQL parser
        // Query example:
        SqlParser parser = SqlParser.create("SELECT * FROM books");
        // SqlParser parser = SqlParser.create("SELECT * FROM books WHERE id = 2");
        // SqlParser parser = SqlParser.create("SELECT DISTINCT author FROM books");
        // SqlParser parser = SqlParser.create("SELECT Count(publish_year) FROM books WHERE publish_year BETWEEN 1800 AND 1850");
        // SqlParser parser = SqlParser.create("SELECT * FROM books WHERE author LIKE 'Victor%'");

        // Parse the query into an AST
//        SqlNode sqlNode = parser.parseQuery();

        // Configure and create a validator
//        Properties props = new Properties();
//        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
//        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
//        CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema, Collections.singletonList(""),
//                typeFactory, config);
//        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
//                catalogReader, typeFactory,
//                SqlValidator.Config.DEFAULT);

        // Validate the initial AST
//        SqlNode validNode = validator.validate(sqlNode);

        // Configure and instantiate the converter of the AST to Logical plan
//        SqlToRelConverter relConverter = new SqlToRelConverter(
//                NOOP_EXPANDER,
//                validator,
//                catalogReader,
//                cluster,
//                StandardConvertletTable.INSTANCE,
//                SqlToRelConverter.config());

        // Convert the valid AST into a logical plan
//        RelNode logicalPlan = relConverter.convertQuery(validNode, false, true).rel;

        // Display the logical plan
//        System.out.println(
//                RelOptUtil.dumpPlan("[Logical plan]", logicalPlan, SqlExplainFormat.TEXT,
//                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        // Initialize optimizer/planner with the necessary rules
//        RelOptPlanner planner = cluster.getPlanner();
//        for (RelOptRule rule : Bindables.RULES) {
//            planner.addRule(rule);
//        }

        // allow pushdown filters
//        planner.addRule(CoreRules.FILTER_SCAN);

        // Define the type of the output plan (in this case we want a physical plan inBindableConvention)
//        logicalPlan = planner.changeTraits(logicalPlan,
//                cluster.traitSet().replace(BindableConvention.INSTANCE));
//        planner.setRoot(logicalPlan);

        // Start the optimization process to obtain the most efficient physical plan based on the
        // provided rule set.
//        BindableRel physicalPlan = (BindableRel) planner.findBestExp();

        // Display the physical plan
//        System.out.println(
//                RelOptUtil.dumpPlan("[Physical plan]", physicalPlan, SqlExplainFormat.TEXT,
//                        SqlExplainLevel.NON_COST_ATTRIBUTES));

        System.out.println("Results:");
        // Run the executable plan using a context simply providing access to the schema
//        for (Object[] row : physicalPlan.bind(new SchemaOnlyDataContext(schema))) {
//            System.out.println(Arrays.toString(row));
//        }
    }

    private Table createBookTable() {
        RelDataTypeFactory.Builder bookType = new RelDataTypeFactory.Builder(typeFactory);
        bookType.add("id", SqlTypeName.INTEGER);
        bookType.add("title", SqlTypeName.VARCHAR);
        bookType.add("publish_year", SqlTypeName.INTEGER);
        bookType.add("author", SqlTypeName.VARCHAR);
        // Initialize books table with data
        return new ListTable(bookType.build(), BOOK_DATA);
        // return new MapTable(bookType.build(), BOOK_DATA);
    }

    /**
     * A simple data context only with schema information.
     */
    private static final class SchemaOnlyDataContext implements DataContext {
        private final SchemaPlus schema;

        SchemaOnlyDataContext(CalciteSchema calciteSchema) {
            this.schema = calciteSchema.plus();
        }

        @Override
        public SchemaPlus getRootSchema() {
            return schema;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl();
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(final String name) {
            return null;
        }
    }
}
