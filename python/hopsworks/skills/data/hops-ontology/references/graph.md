# The ontology graph

`<slug>-graph` rebuilds the graph from the app's tables and publishes it at `/hopsfs/<SYSTEM_DIR>/relate.db`, a relate directory store whose write-ahead log is empty.
It keeps the labels and relationship types of NVIDIA Auto-Ontology, whose store these tables were ported from.
Every node has the `id` of its row in the app's tables; a property whose value is null is absent.

## Nodes

| Label | What it is | Properties |
| --- | --- | --- |
| `Ontology` | One per graph: what was published, and when | `name` (the system slug), `published_at` (UTC, ISO 8601), `graph_model` (1) |
| `Database` | A source database | `name`, `dialect`, `description`, `description_certified`, `owner_id`, `connection` (JSON: the Hopsworks data source name and its schema and table filters, no credentials) |
| `Schema` | A schema in a database | `name`, `description`, `description_certified` |
| `Table` | A table or view | `name`, `description`, `description_certified`, `owner_id`, `pk` (list of column names), `table_type`, `is_junction_table` |
| `Column` | A column | `name`, `description`, `description_certified`, `data_type`, `format` (how values are written, such as `YYYY-MM-DD`), `is_unique`, `is_nullable`, `ordinal_position` |
| `Term` | A business term | `name`, `description`, `source`, `synonyms` (list), `name_certified`, `description_certified`, `owner_id` |
| `ColumnAttribute` | A term's property, held by a column | `name`, `description`, `datatype`, `source`, `source_column`, `term_name`, `certified` |
| `SqlAttribute` | A term's property as a reviewed SQL expression, such as a measure | `name`, `description`, `expression`, `source` (`manual`, `sql`, `table`, `bridgeTable`), `certified`, `owner_id` |
| `Sql` | A statement seen in the query history | `sql_full_query`, `total_counter`, `last_seen`, `join_count`, `union_count` |
| `CustomAnalysis` | A saved SQL analysis | `name`, `description` |
| `PqlAnalysis` | A saved predictive (PQL) analysis | `name`, `description`, `pql` |
| `Tag` | A label such as `PII` | `name` |
| `Acronym` | A glossary acronym | `name`, `description` |

Columns carry no sample values: the app withholds them from Observers, who can read the system's directory.

## Relationships

| Type | From, to | Properties |
| --- | --- | --- |
| `CONTAINS` | `Database` to `Schema`, `Schema` to `Table`, `Table` to `Column` | |
| `FOREIGN_KEY` | `Column` to the `Column` it references | |
| `JOIN` | `Column` to `Column`, a join seen in the query history | `refs` (list of `<sql id>\|<condition>`) |
| `UNION` | `Column` to `Column`, a union seen in the query history | `refs` |
| `JOIN` | `Table` to `Table`, from an imported model | `join_columns` (JSON) |
| `SQL` | `Sql` to each `Table` and `Column` it reads | |
| `REPRESENTS` | `Table` to the `Term` it holds rows of | |
| `HAS_ATTRIBUTE` | `Column` to the `ColumnAttribute` it is an instance of | |
| `SEMANTIC_FK` | `Column` to the `ColumnAttribute` on another table it refers to | `use_in_join_paths` |
| `PROPERTY_OF` | `ColumnAttribute` or `SqlAttribute` to its `Term` | |
| `HAS_SQL` | `SqlAttribute` or `CustomAnalysis` to its `Sql` | |
| `TAGGED` | `Term`, `Table`, `Column`, `ColumnAttribute` or `SqlAttribute` to a `Tag` | `tagged_by` (a username), `rule` (the rule that applied it) |

## Queries

```cypher
// What the graph holds
MATCH (n) UNWIND labels(n) AS label RETURN label, count(*) AS n ORDER BY label;

// The tables behind a business term, matched on its name or a synonym
MATCH (t:Table)-[:REPRESENTS]->(term:Term)
WHERE toLower(term.name) = 'customer' OR 'customer' IN [s IN term.synonyms | toLower(s)]
RETURN t.name AS table, term.description AS meaning;

// A term's reviewed measures
MATCH (a:SqlAttribute)-[:PROPERTY_OF]->(:Term {name: 'Order'})
RETURN a.name AS measure, a.expression AS sql, a.certified AS certified;

// How two tables join: declared foreign keys, then joins seen in the query history
MATCH (:Table {name: 'orders'})-[:CONTAINS]->(c:Column)-[:FOREIGN_KEY|JOIN]-(k:Column)<-[:CONTAINS]-(t:Table {name: 'customers'})
RETURN c.name AS orders_column, k.name AS customers_column;

// Every column tagged PII, with its table
MATCH (t:Table)-[:CONTAINS]->(c:Column)-[:TAGGED]->(:Tag {name: 'PII'})
RETURN t.name AS table, c.name AS column;
```

From the CLI, end each statement with `;` to run several at once, and add `--json` for rows a script can parse.
