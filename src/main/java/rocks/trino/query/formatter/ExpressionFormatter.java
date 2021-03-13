package rocks.trino.query.formatter;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AllRows;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IntervalDayTimeDataType;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalBinaryExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.TypeParameter;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.PrimitiveIterator;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static rocks.trino.query.formatter.SqlFormatter.formatName;
import static rocks.trino.query.formatter.SqlFormatter.formatSql;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class ExpressionFormatter
{
    private static final ThreadLocal<DecimalFormat> doubleFormatter = ThreadLocal.withInitial(
            () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

    private ExpressionFormatter() {}

    public static String formatExpression(Expression expression, Integer indent)
    {
        return new Formatter().process(expression, indent);
    }

    private static String formatIdentifier(String s)
    {
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    public static class Formatter
            extends AstVisitor<String, Integer>
    {
        private boolean shouldIndent = true;

        private Integer maybeIndent(Integer indent) {
            return shouldIndent
                    ? indent + 4
                    : indent;
        }

        @Override
        protected String visitNode(Node node, Integer indent)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitRow(Row node, Integer indent)
        {
            return "ROW (" + Joiner.on(", ").join(node.getItems().stream()
                    .map((child) -> process(child, indent))
                    .collect(toList())) + ")";
        }

        @Override
        protected String visitExpression(Expression node, Integer indent)
        {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitAtTimeZone(AtTimeZone node, Integer indent)
        {
            return new StringBuilder()
                    .append(process(node.getValue(), indent))
                    .append(" AT TIME ZONE ")
                    .append(process(node.getTimeZone(), indent)).toString();
        }

        @Override
        protected String visitCurrentUser(CurrentUser node, Integer indent)
        {
            return "CURRENT_USER";
        }

        @Override
        protected String visitCurrentPath(CurrentPath node, Integer indent)
        {
            return "CURRENT_PATH";
        }

        @Override
        protected String visitFormat(Format node, Integer indent)
        {
            return "format(" + joinExpressions(node.getArguments()) + ")";
        }

        @Override
        protected String visitCurrentTime(CurrentTime node, Integer indent)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getFunction().getName());

            if (node.getPrecision() != null) {
                builder.append('(')
                        .append(node.getPrecision())
                        .append(')');
            }

            return builder.toString();
        }

        @Override
        protected String visitExtract(Extract node, Integer indent)
        {
            return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), indent) + ")";
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Integer indent)
        {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Integer indent)
        {
            return formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitCharLiteral(CharLiteral node, Integer indent)
        {
            return "CHAR " + formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitBinaryLiteral(BinaryLiteral node, Integer indent)
        {
            return "X'" + node.toHexString() + "'";
        }

        @Override
        protected String visitParameter(Parameter node, Integer indent)
        {
            return "?";
        }

        @Override
        protected String visitAllRows(AllRows node, Integer indent)
        {
            return "ALL";
        }

        @Override
        protected String visitArrayConstructor(ArrayConstructor node, Integer indent)
        {
            ImmutableList.Builder<String> valueStrings = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                valueStrings.add(formatSql(value));
            }
            return "ARRAY[" + Joiner.on(",").join(valueStrings.build()) + "]";
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Integer indent)
        {
            return formatSql(node.getBase()) + "[" + formatSql(node.getIndex()) + "]";
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Integer indent)
        {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Integer indent)
        {
            return doubleFormatter.get().format(node.getValue());
        }

        @Override
        protected String visitDecimalLiteral(DecimalLiteral node, Integer indent)
        {
            // TODO return node value without "DECIMAL '..'" when FeaturesConfig#parseDecimalLiteralsAsDouble switch is removed
            return "DECIMAL '" + node.getValue() + "'";
        }

        @Override
        protected String visitGenericLiteral(GenericLiteral node, Integer indent)
        {
            return node.getType() + " " + formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitTimeLiteral(TimeLiteral node, Integer indent)
        {
            return "TIME '" + node.getValue() + "'";
        }

        @Override
        protected String visitTimestampLiteral(TimestampLiteral node, Integer indent)
        {
            return "TIMESTAMP '" + node.getValue() + "'";
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Integer indent)
        {
            return "null";
        }

        @Override
        protected String visitIntervalLiteral(IntervalLiteral node, Integer indent)
        {
            String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? " -" : "";
            StringBuilder builder = new StringBuilder()
                    .append("INTERVAL")
                    .append(sign)
                    .append(" '").append(node.getValue()).append("' ")
                    .append(node.getStartField());

            if (node.getEndField().isPresent()) {
                builder.append(" TO ").append(node.getEndField().get());
            }
            return builder.toString();
        }

        @Override
        protected String visitSubqueryExpression(SubqueryExpression node, Integer indent)
        {
            return "(" + formatSql(node.getQuery()) + ")";
        }

        @Override
        protected String visitExists(ExistsPredicate node, Integer indent)
        {
            return "(EXISTS " + formatSql(node.getSubquery()) + ")";
        }

        @Override
        protected String visitIdentifier(Identifier node, Integer indent)
        {
            if (!node.isDelimited()) {
                return node.getValue();
            }
            else {
                return '"' + node.getValue().replace("\"", "\"\"") + '"';
            }
        }

        @Override
        protected String visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, Integer indent)
        {
            return formatExpression(node.getName(), indent);
        }

        @Override
        protected String visitSymbolReference(SymbolReference node, Integer indent)
        {
            return formatIdentifier(node.getName());
        }

        @Override
        protected String visitDereferenceExpression(DereferenceExpression node, Integer indent)
        {
            String baseString = process(node.getBase(), indent);
            return baseString + "." + process(node.getField());
        }

        @Override
        public String visitFieldReference(FieldReference node, Integer indent)
        {
            // add colon so this won't parse
            return ":input(" + node.getFieldIndex() + ")";
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Integer indent)
        {
            StringBuilder builder = new StringBuilder();

            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            builder.append(formatName(node.getName()))
                    .append('(').append(arguments);

            if (node.getOrderBy().isPresent()) {
                builder.append(' ').append(formatOrderBy(node.getOrderBy().get(), indent));
            }

            builder.append(')');

            node.getNullTreatment().ifPresent(nullTreatment -> {
                switch (nullTreatment) {
                    case IGNORE:
                        builder.append(" IGNORE NULLS");
                        break;
                    case RESPECT:
                        builder.append(" RESPECT NULLS");
                        break;
                }
            });

            if (node.getFilter().isPresent()) {
                builder.append(" FILTER ").append(visitFilter(node.getFilter().get(), indent));
            }

            if (node.getWindow().isPresent()) {
                builder.append(" OVER ").append(formatWindow(node.getWindow().get(), indent));
            }

            return builder.toString();
        }

        @Override
        protected String visitLambdaExpression(LambdaExpression node, Integer indent)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(');
            Joiner.on(", ").appendTo(builder, node.getArguments());
            builder.append(") -> ");
            builder.append(process(node.getBody(), indent));
            return builder.toString();
        }

        @Override
        protected String visitBindExpression(BindExpression node, Integer indent)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("\"$INTERNAL$BIND\"(");
            for (Expression value : node.getValues()) {
                builder.append(process(value, indent))
                        .append(", ");
            }
            builder.append(process(node.getFunction(), indent))
                    .append(")");
            return builder.toString();
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Integer indent)
        {
            return formatBinaryExpression(node.getOperator().toString(), node.getLeft(), node.getRight(), indent);
        }

        @Override
        protected String visitNotExpression(NotExpression node, Integer indent)
        {
            return "(NOT " + process(node.getValue(), indent) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Integer indent)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight(), indent);
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Integer indent)
        {
            return process(node.getValue(), indent) + " IS NULL";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Integer indent)
        {
            return process(node.getValue(), indent) + " IS NOT NULL";
        }

        @Override
        protected String visitNullIfExpression(NullIfExpression node, Integer indent)
        {
            return "NULLIF(" + process(node.getFirst(), indent) + ", " + process(node.getSecond(), indent) + ')';
        }

        @Override
        protected String visitIfExpression(IfExpression node, Integer indent)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("IF(")
                    .append(process(node.getCondition(), indent))
                    .append(", ")
                    .append(process(node.getTrueValue(), indent));
            if (node.getFalseValue().isPresent()) {
                builder.append(", ")
                        .append(process(node.getFalseValue().get(), indent));
            }
            builder.append(")");
            return builder.toString();
        }

        @Override
        protected String visitTryExpression(TryExpression node, Integer indent)
        {
            return "TRY(" + process(node.getInnerExpression(), indent) + ")";
        }

        @Override
        protected String visitCoalesceExpression(CoalesceExpression node, Integer indent)
        {
            return "COALESCE(" + joinExpressions(node.getOperands()) + ")";
        }

        @Override
        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Integer indent)
        {
            String value = process(node.getValue(), indent);

            switch (node.getSign()) {
                case MINUS:
                    // Unary is ambiguous with respect to negative numbers. "-1" parses as a number, but "-(1)" parses as "unaryMinus(number)"
                    // The parentheses are needed to ensure the parsing roundtrips properly.
                    return "-(" + value + ")";
                case PLUS:
                    return "+" + value;
            }
            throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
        }

        @Override
        protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Integer indent)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight(), indent);
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Integer indent)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(process(node.getValue(), indent))
                    .append(" LIKE ")
                    .append(process(node.getPattern(), indent));

            node.getEscape().ifPresent(escape -> builder.append(" ESCAPE ")
                    .append(process(escape, indent)));

            return builder.toString();
        }

        @Override
        protected String visitAllColumns(AllColumns node, Integer indent)
        {
            StringBuilder builder = new StringBuilder();
            if (node.getTarget().isPresent()) {
                builder.append(process(node.getTarget().get(), indent));
                builder.append(".*");
            }
            else {
                builder.append("*");
            }

            if (!node.getAliases().isEmpty()) {
                builder.append(" AS (");
                Joiner.on(", ").appendTo(builder, node.getAliases().stream()
                        .map(alias -> process(alias, indent))
                        .collect(toList()));
                builder.append(")");
            }

            return builder.toString();
        }

        @Override
        public String visitCast(Cast node, Integer indent)
        {
            return (node.isSafe() ? "TRY_CAST" : "CAST") +
                    "(" + process(node.getExpression(), indent) + " AS " + process(node.getType(), indent) + ")";
        }

        @Override
        protected String visitSearchedCaseExpression(SearchedCaseExpression node, Integer indent)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();
            parts.add("CASE");
            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, indent));
            }

            node.getDefaultValue()
                    .ifPresent((value) -> parts.add("ELSE").add(process(value, indent)));

            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitSimpleCaseExpression(SimpleCaseExpression node, Integer indent)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            parts.add("CASE")
                    .add(process(node.getOperand(), indent));

            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, indent));
            }

            node.getDefaultValue()
                    .ifPresent((value) -> parts.add("ELSE").add(process(value, indent)));

            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitWhenClause(WhenClause node, Integer indent)
        {
            return "WHEN " + process(node.getOperand(), indent) + " THEN " + process(node.getResult(), indent);
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Integer indent)
        {
            return process(node.getValue(), indent) + " BETWEEN " +
                    process(node.getMin(), indent) + " AND " + process(node.getMax(), indent);
        }

        @Override
        protected String visitInPredicate(InPredicate node, Integer indent)
        {
            return process(node.getValue(), indent) + " IN " + process(node.getValueList(), indent);
        }

        @Override
        protected String visitInListExpression(InListExpression node, Integer indent)
        {
            return joinExpressions(node.getValues());
        }

        private String visitFilter(Expression node, Integer indent)
        {
            return "WHERE " + process(node, indent);
        }

        @Override
        protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Integer indent)
        {
            return new StringBuilder()
                    .append("(")
                    .append(process(node.getValue(), indent))
                    .append(' ')
                    .append(node.getOperator().getValue())
                    .append(' ')
                    .append(node.getQuantifier().toString())
                    .append(' ')
                    .append(process(node.getSubquery(), indent))
                    .append(")")
                    .toString();
        }

        @Override
        protected String visitGroupingOperation(GroupingOperation node, Integer indent)
        {
            return "GROUPING (" + joinExpressions(node.getGroupingColumns()) + ")";
        }

        @Override
        protected String visitRowDataType(RowDataType node, Integer indent)
        {
            return node.getFields().stream()
                    .map(this::process)
                    .collect(joining(", ", "ROW(", ")"));
        }

        @Override
        protected String visitRowField(RowDataType.Field node, Integer indent)
        {
            StringBuilder result = new StringBuilder();

            if (node.getName().isPresent()) {
                result.append(process(node.getName().get(), indent));
                result.append(" ");
            }

            result.append(process(node.getType(), indent));

            return result.toString();
        }

        @Override
        protected String visitGenericDataType(GenericDataType node, Integer indent)
        {
            StringBuilder result = new StringBuilder();
            result.append(node.getName());

            if (!node.getArguments().isEmpty()) {
                result.append(node.getArguments().stream()
                        .map(this::process)
                        .collect(joining(", ", "(", ")")));
            }

            return result.toString();
        }

        @Override
        protected String visitTypeParameter(TypeParameter node, Integer indent)
        {
            return process(node.getValue(), indent);
        }

        @Override
        protected String visitNumericTypeParameter(NumericParameter node, Integer indent)
        {
            return node.getValue();
        }

        @Override
        protected String visitIntervalDataType(IntervalDayTimeDataType node, Integer indent)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("INTERVAL ");
            builder.append(node.getFrom());
            if (node.getFrom() != node.getTo()) {
                builder.append(" TO ")
                        .append(node.getTo());
            }

            return builder.toString();
        }

        @Override
        protected String visitDateTimeType(DateTimeDataType node, Integer indent)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getType().toString().toLowerCase(Locale.ENGLISH)); // TODO: normalize to upper case according to standard SQL semantics
            if (node.getPrecision().isPresent()) {
                builder.append("(")
                        .append(node.getPrecision().get())
                        .append(")");
            }

            if (node.isWithTimeZone()) {
                builder.append(" with time zone"); // TODO: normalize to upper case according to standard SQL semantics
            }

            return builder.toString();
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right, Integer indent)
        {
            switch (operator) {
                case "AND":
                case "OR":
                    if (!shouldIndent) {
                        return process(left, indent) + ' ' + operator + ' ' + process(right, indent);
                    }
                    return process(left, indent) + '\n' + Strings.repeat("  ", indent) + operator + ' ' + process(right, indent);

                default:
                    return process(left, indent) + ' ' + operator + ' ' + process(right, indent);
            }
        }

        private String joinExpressions(List<Expression> expressions)
        {
            return Joiner.on(", ").join(expressions.stream()
                    .map((e) -> process(e, null))
                    .iterator());
        }
    }

    static String formatStringLiteral(String s)
    {
        s = s.replace("'", "''");
        if (CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(s)) {
            return "'" + s + "'";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("U&'");
        PrimitiveIterator.OfInt iterator = s.codePoints().iterator();
        while (iterator.hasNext()) {
            int codePoint = iterator.nextInt();
            checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", s);
            if (isAsciiPrintable(codePoint)) {
                char ch = (char) codePoint;
                if (ch == '\\') {
                    builder.append(ch);
                }
                builder.append(ch);
            }
            else if (codePoint <= 0xFFFF) {
                builder.append('\\');
                builder.append(format("%04X", codePoint));
            }
            else {
                builder.append("\\+");
                builder.append(format("%06X", codePoint));
            }
        }
        builder.append("'");
        return builder.toString();
    }

    public static String formatOrderBy(OrderBy orderBy, Integer indent)
    {
        return "ORDER BY " + formatSortItems(orderBy.getSortItems(), indent);
    }

    private static String formatSortItems(List<SortItem> sortItems, Integer indent)
    {
        return Joiner.on(", ").join(sortItems.stream()
                .map(sortItemFormatterFunction(indent))
                .iterator());
    }

    private static String formatWindow(Window window, Integer indent)
    {
        if (window instanceof WindowReference) {
            return formatExpression(((WindowReference) window).getName(), indent);
        }

        return formatWindowSpecification((WindowSpecification) window, indent);
    }

    static String formatWindowSpecification(WindowSpecification windowSpecification, Integer indent)
    {
        List<String> parts = new ArrayList<>();

        if (windowSpecification.getExistingWindowName().isPresent()) {
            parts.add(formatExpression(windowSpecification.getExistingWindowName().get(), indent));
        }
        if (!windowSpecification.getPartitionBy().isEmpty()) {
            parts.add("PARTITION BY " + windowSpecification.getPartitionBy().stream()
                    .map(e -> formatExpression(e, indent))
                    .collect(joining(", ")));
        }
        if (windowSpecification.getOrderBy().isPresent()) {
            parts.add(formatOrderBy(windowSpecification.getOrderBy().get(), indent));
        }
        if (windowSpecification.getFrame().isPresent()) {
            parts.add(formatFrame(windowSpecification.getFrame().get(), indent));
        }

        return '(' + Joiner.on(' ').join(parts) + ')';
    }

    private static String formatFrame(WindowFrame windowFrame, Integer indent)
    {
        StringBuilder builder = new StringBuilder();

        builder.append(windowFrame.getType().toString())
                .append(' ');

        if (windowFrame.getEnd().isPresent()) {
            builder.append("BETWEEN ")
                    .append(formatFrameBound(windowFrame.getStart(), indent))
                    .append(" AND ")
                    .append(formatFrameBound(windowFrame.getEnd().get(), indent));
        }
        else {
            builder.append(formatFrameBound(windowFrame.getStart(), indent));
        }

        return builder.toString();
    }

    private static String formatFrameBound(FrameBound frameBound, Integer indent)
    {
        switch (frameBound.getType()) {
            case UNBOUNDED_PRECEDING:
                return "UNBOUNDED PRECEDING";
            case PRECEDING:
                return formatExpression(frameBound.getValue().get(), indent) + " PRECEDING";
            case CURRENT_ROW:
                return "CURRENT ROW";
            case FOLLOWING:
                return formatExpression(frameBound.getValue().get(), indent) + " FOLLOWING";
            case UNBOUNDED_FOLLOWING:
                return "UNBOUNDED FOLLOWING";
        }
        throw new IllegalArgumentException("unhandled type: " + frameBound.getType());
    }

    static String formatGroupBy(List<GroupingElement> groupingElements, Integer indent)
    {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        for (GroupingElement groupingElement : groupingElements) {
            String result = "";
            if (groupingElement instanceof SimpleGroupBy) {
                List<Expression> columns = groupingElement.getExpressions();
                if (columns.size() == 1) {
                    result = formatExpression(getOnlyElement(columns), indent);
                }
                else {
                    result = formatGroupingSet(columns, indent);
                }
            }
            else if (groupingElement instanceof GroupingSets) {
                result = format("GROUPING SETS (%s)", Joiner.on(", ").join(
                        ((GroupingSets) groupingElement).getSets().stream()
                                .map(groupingSet -> formatGroupingSet(groupingSet, indent))
                                .iterator()));
            }
            else if (groupingElement instanceof Cube) {
                result = format("CUBE %s", formatGroupingSet(groupingElement.getExpressions(), indent));
            }
            else if (groupingElement instanceof Rollup) {
                result = format("ROLLUP %s", formatGroupingSet(groupingElement.getExpressions(), indent));
            }
            resultStrings.add(result);
        }
        return Joiner.on(", ").join(resultStrings.build());
    }

    private static boolean isAsciiPrintable(int codePoint)
    {
        return codePoint >= 0x20 && codePoint < 0x7F;
    }

    private static String formatGroupingSet(List<Expression> groupingSet, Integer indent)
    {
        return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                .map(e -> formatExpression(e, indent))
                .iterator()));
    }

    private static Function<SortItem, String> sortItemFormatterFunction(Integer indent)
    {
        return input -> {
            StringBuilder builder = new StringBuilder();

            builder.append(formatExpression(input.getSortKey(), indent));

            switch (input.getOrdering()) {
                case ASCENDING:
                    builder.append(" ASC");
                    break;
                case DESCENDING:
                    builder.append(" DESC");
                    break;
                default:
                    throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
            }

            switch (input.getNullOrdering()) {
                case FIRST:
                    builder.append(" NULLS FIRST");
                    break;
                case LAST:
                    builder.append(" NULLS LAST");
                    break;
                case UNDEFINED:
                    // no op
                    break;
                default:
                    throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
            }

            return builder.toString();
        };
    }
}
