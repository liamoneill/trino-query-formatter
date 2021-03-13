package rocks.trino.query.formatter;

import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Relation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableVisitor extends AstVisitor<Void, Void>
{
    private final List<String> tableIdentifiers = new ArrayList<>();

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, Void context) {
        tableIdentifiers.add(node.getAlias().toString());
        return super.visitAliasedRelation(node, context);
    }

    @Override
    protected Void visitRelation(Relation node, Void context) {
        return super.visitRelation(node, context);
    }

    public List<String> getTableNames() {
        return Collections.unmodifiableList(tableIdentifiers);
    }
}