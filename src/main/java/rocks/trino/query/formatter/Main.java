package rocks.trino.query.formatter;

import com.google.common.base.CharMatcher;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.intigua.antlr4.autosuggest.AutoSuggester;
import com.intigua.antlr4.autosuggest.LexerAndParserFactory;
import com.intigua.antlr4.autosuggest.ReflectionLexerAndParserFactory;
import io.trino.sql.parser.CaseInsensitiveStream;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlBaseLexer;
import io.trino.sql.parser.SqlBaseParser;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static spark.Spark.post;

public class Main
{
    private static final Collection<String> KNOWN_TABLES = Collections.unmodifiableList(Arrays.asList(
            "profiles",
            "events",
            "orders",
            "carts",
            "returns"));

    private static final Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    private static final ParsingOptions PARSING_OPTIONS = new ParsingOptions();
    private static final SqlParser SQL_PARSER = new SqlParser();

    private Main()
    {
    }

    public static void main(String[] args)
    {
        post("/v1/parse", (req, res) -> {
            return parse(GSON.fromJson(req.body(), Request.class));
        }, GSON::toJson);
    }

    public static Response parse(Request request)
    {
        String sql = request.sql;
        sql = stripSemicolon(sql);

        String formattedSql = null;
        Response.ParseError parseError = null;
        Collection<String> suggestions = Collections.emptyList();

        try {
            Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);
            suggestions = suggestionsFromStatement(statement, sql);

            formattedSql = formatSql(statement);
        }
        catch (ParsingException e) {
            suggestions = suggestionsFromParsingException(e, sql);
            parseError = new Response.ParseError(
                    e.getErrorMessage(),
                    e.getLineNumber(),
                    e.getColumnNumber());
        }

        Collection<String> autoSuggestions = request.includeAutoSuggestions
                ? autoCompleteSuggestions(sql)
                : null;

        return new Response(formattedSql, suggestions, autoSuggestions, parseError);
    }

    private static Collection<String> suggestionsFromStatement(Statement statement, String sql)
    {
        TableVisitor tableVisitor = new TableVisitor();
        tableVisitor.process(statement);

        Matcher matcher;

        matcher = Pattern.compile("^.*\\s(FROM|JOIN)\\s+(?<table>\\w+)$", Pattern.CASE_INSENSITIVE).matcher(sql);
        if (matcher.matches()) {
            String table = matcher.group("table");

            return Stream.concat(KNOWN_TABLES.stream(), tableVisitor.getTableNames().stream())
                    .filter(t -> t.startsWith(table))
                    .collect(Collectors.toList());
        }

        matcher = Pattern.compile("^.*\\s+(?<table>\\w+)\\.(?<column>\\w+)$", Pattern.CASE_INSENSITIVE).matcher(sql);
        if (matcher.matches()) {
            String table = matcher.group("table");
            String column = matcher.group("column");

            if (table.equals("events")) {
                // TODO figure out how to retrieve schema from Trino
                return Arrays.asList("id", "type", "event_time");
            }
        }

        return Collections.emptyList();
    }

    private static String stripSemicolon(String sql)
    {
        return CharMatcher.whitespace()
                .or(CharMatcher.is(';'))
                .trimTrailingFrom(sql);
    }

    private static String formatSql(Statement sql)
    {
        String formattedSql = SqlFormatter.formatSql(sql);

        // Check that the original & formatted SQL statements are logically equivalent
        checkState(sql.equals(SQL_PARSER.createStatement(formattedSql, PARSING_OPTIONS)), "Formatted SQL is different than original");

        formattedSql = CharMatcher.is('\n').trimTrailingFrom(formattedSql);
        formattedSql = formattedSql + '\n';

        return formattedSql;
    }

    private static Collection<String> suggestionsFromParsingException(ParsingException e, String sql)
    {
        String errorMessage = e.getErrorMessage();

        Collection<String> suggestions = Collections.emptyList();

        if (errorMessage.startsWith("mismatched input '<EOF>'. Expecting: ")) {
            suggestions = suggestionsFromEofError(errorMessage, sql);
        } else if (errorMessage.matches("^mismatched input '.*'\\. Expecting: .+")) {
            suggestions = suggestionsFromMismatchedInputError(errorMessage);
        }

        return suggestions;
    }

    private static Collection<String> suggestionsFromMismatchedInputError(String errorMessage)
    {
        String[] expectingTokens = errorMessage.replaceFirst("^mismatched input '.*'\\. Expecting: ", "")
                .split(", ");

        return Arrays.stream(expectingTokens)
                .sorted((t1, t2) -> {
                    if (t1.startsWith("<") && !t2.startsWith("<")) {
                        return -1;
                    }
                    if (!t1.startsWith("<") && t2.startsWith("<")) {
                        return 1;
                    }
                    return t1.compareTo(t2);
                })
                .flatMap(token -> token.equals("<query>")
                        ? Stream.of("SELECT", "WITH")
                        : Stream.of(token))
                .map(token -> {
                    token = CharMatcher.is('\'').trimLeadingFrom(token);
                    token = CharMatcher.is('\'').trimTrailingFrom(token);
                    return token;
                })
                .collect(Collectors.toList());
    }

    private static Collection<String> suggestionsFromEofError(String errorMessage, String sql)
    {
        String[] expectingTokens = errorMessage
                .replace("mismatched input '<EOF>'. Expecting: ", "")
                .split(", ");

        return Arrays.stream(expectingTokens)
                .sorted((t1, t2) -> {
                    if (t1.startsWith("<") && !t2.startsWith("<")) {
                        return -1;
                    }
                    if (!t1.startsWith("<") && t2.startsWith("<")) {
                        return 1;
                    }
                    return t1.compareTo(t2);
                })
                .map(token -> {
                    token = CharMatcher.is('\'').trimLeadingFrom(token);
                    token = CharMatcher.is('\'').trimTrailingFrom(token);
                    return token;
                })
                .flatMap(token -> token.equals("<identifier>") && (
                        sql.trim().toUpperCase().endsWith("FROM") || sql.trim().toUpperCase().endsWith("JOIN"))
                        ? KNOWN_TABLES.stream()
                        : Stream.of(token))
                .collect(Collectors.toList());
    }

    private static Collection<String> autoCompleteSuggestions(String sql)
    {
        LexerAndParserFactory lexerAndParserFactory = new ReflectionLexerAndParserFactory(SqlBaseLexer.class, SqlBaseParser.class) {
            @Override
            public Lexer createLexer(CharStream input)
            {
                return super.createLexer(new CaseInsensitiveStream(input));
            }
        };
        AutoSuggester suggester = new AutoSuggester(lexerAndParserFactory, sql);
        return suggester.suggestCompletions();
    }

    public static class Request
    {
        public String sql;
        public boolean includeAutoSuggestions;

        public Request(String sql, boolean includeAutoSuggestions)
        {
            this.sql = sql;
            this.includeAutoSuggestions = includeAutoSuggestions;
        }
    }

    public static class Response
    {
        public String formattedSql;
        public Collection<String> suggestions;
        public Collection<String> autoSuggestions;
        public ParseError parseError;

        public Response(String formattedSql, Collection<String> suggestions, Collection<String> autoSuggestions, ParseError parseError)
        {
            this.formattedSql = formattedSql;
            this.suggestions = suggestions;
            this.autoSuggestions = autoSuggestions;
            this.parseError = parseError;
        }

        public static class ParseError
        {
            public String message;
            public int row;
            public int column;

            public ParseError(String message, int row, int column)
            {
                this.message = message;
                this.row = row;
                this.column = column;
            }
        }
    }
}
