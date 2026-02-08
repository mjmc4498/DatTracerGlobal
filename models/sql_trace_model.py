import re
from dataclasses import dataclass
from typing import Dict, List, Set

from models.lineage_model import LineageModel
from models.traceability_model import TraceabilityModel


@dataclass
class TraceResult:
    statement: str
    category: str
    action: str
    objects: List[str]
    clauses: List[str]
    functions: List[str]


class SqlTraceModel:
    DDL_ACTIONS = {
        "CREATE DATABASE",
        "DROP DATABASE",
        "CREATE SCHEMA",
        "DROP SCHEMA",
        "CREATE TABLE",
        "DROP TABLE",
        "TRUNCATE TABLE",
        "ALTER TABLE",
        "RENAME TABLE",
        "CREATE VIEW",
        "DROP VIEW",
        "CREATE INDEX",
        "DROP INDEX",
        "CREATE SEQUENCE",
        "DROP SEQUENCE",
        "CREATE FUNCTION",
        "DROP FUNCTION",
        "CREATE PROCEDURE",
        "DROP PROCEDURE",
        "CREATE TRIGGER",
        "DROP TRIGGER",
    }
    DML_ACTIONS = {"SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"}
    DCL_ACTIONS = {"GRANT", "REVOKE"}
    TCL_ACTIONS = {
        "BEGIN",
        "START TRANSACTION",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "SET TRANSACTION",
    }
    UTILITY_ACTIONS = {"DESCRIBE", "EXPLAIN", "EXPLAIN ANALYZE", "SHOW", "USE"}

    CLAUSES = {
        "FROM",
        "WHERE",
        "GROUP BY",
        "HAVING",
        "ORDER BY",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "DISTINCT",
        "AS",
        "JOIN",
        "INNER JOIN",
        "LEFT JOIN",
        "RIGHT JOIN",
        "FULL JOIN",
        "CROSS JOIN",
        "SELF JOIN",
        "PARTITION BY",
    }

    FUNCTIONS = {
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "UPPER",
        "LOWER",
        "LENGTH",
        "SUBSTRING",
        "TRIM",
        "COALESCE",
        "NULLIF",
        "CAST",
        "CONVERT",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "NOW",
        "DATEADD",
        "DATEDIFF",
        "EXTRACT",
        "OVER",
        "ROW_NUMBER",
        "RANK",
        "DENSE_RANK",
        "LAG",
        "LEAD",
    }

    def __init__(self) -> None:
        self._traceability = TraceabilityModel()
        self._lineage = LineageModel()

    def analyze(self, sql_text: str) -> Dict[str, object]:
        statements = self._split_statements(sql_text)
        traces: List[TraceResult] = []
        traceability_rows: List[Dict[str, object]] = []
        lineage_edges: List[Dict[str, str]] = []
        lineage_nodes: Set[str] = set()

        for statement in statements:
            action = self._detect_action(statement)
            category = self._detect_category(action)
            objects = self._extract_objects(statement, action)
            clauses = self._detect_clauses(statement)
            functions = self._detect_functions(statement)

            traces.append(
                TraceResult(
                    statement=statement,
                    category=category,
                    action=action,
                    objects=objects,
                    clauses=clauses,
                    functions=functions,
                )
            )

            traceability_rows.extend(self._traceability.analyze(statement))

            lineage = self._lineage.analyze(statement, action, objects)
            if lineage:
                lineage_nodes.update(lineage["nodes"])
                lineage_edges.extend(lineage["edges"])

        return {
            "traceability": traceability_rows,
            "statement_summary": [trace.__dict__ for trace in traces],
            "lineage": {
                "nodes": sorted(lineage_nodes),
                "edges": lineage_edges,
            },
        }

    def _split_statements(self, sql_text: str) -> List[str]:
        cleaned = sql_text.replace("\n", " ")
        statements = [segment.strip() for segment in cleaned.split(";")]
        return [statement for statement in statements if statement]

    def _detect_action(self, statement: str) -> str:
        normalized = self._normalize(statement)
        priorities = (
            sorted(self.UTILITY_ACTIONS, key=len, reverse=True)
            + sorted(self.TCL_ACTIONS, key=len, reverse=True)
            + sorted(self.DDL_ACTIONS, key=len, reverse=True)
            + sorted(self.DML_ACTIONS, key=len, reverse=True)
            + sorted(self.DCL_ACTIONS, key=len, reverse=True)
        )
        for keyword in priorities:
            if normalized.startswith(keyword):
                return keyword
        return normalized.split(" ")[0] if normalized else "UNKNOWN"

    def _detect_category(self, action: str) -> str:
        if action in self.DDL_ACTIONS:
            return "DDL"
        if action in self.DML_ACTIONS:
            return "DML"
        if action in self.DCL_ACTIONS:
            return "DCL"
        if action in self.TCL_ACTIONS:
            return "TCL"
        if action in self.UTILITY_ACTIONS:
            return "UTILITY"
        return "UNKNOWN"

    def _extract_objects(self, statement: str, action: str) -> List[str]:
        patterns = {
            "CREATE TABLE": r"CREATE\s+TABLE\s+([^\s(]+)",
            "DROP TABLE": r"DROP\s+TABLE\s+([^\s;]+)",
            "TRUNCATE TABLE": r"TRUNCATE\s+TABLE\s+([^\s;]+)",
            "ALTER TABLE": r"ALTER\s+TABLE\s+([^\s;]+)",
            "RENAME TABLE": r"RENAME\s+TABLE\s+([^\s;]+)",
            "CREATE VIEW": r"CREATE\s+VIEW\s+([^\s;]+)",
            "DROP VIEW": r"DROP\s+VIEW\s+([^\s;]+)",
            "CREATE INDEX": r"CREATE\s+INDEX\s+([^\s;]+)",
            "DROP INDEX": r"DROP\s+INDEX\s+([^\s;]+)",
            "CREATE SEQUENCE": r"CREATE\s+SEQUENCE\s+([^\s;]+)",
            "DROP SEQUENCE": r"DROP\s+SEQUENCE\s+([^\s;]+)",
            "CREATE FUNCTION": r"CREATE\s+FUNCTION\s+([^\s(]+)",
            "DROP FUNCTION": r"DROP\s+FUNCTION\s+([^\s(]+)",
            "CREATE PROCEDURE": r"CREATE\s+PROCEDURE\s+([^\s(]+)",
            "DROP PROCEDURE": r"DROP\s+PROCEDURE\s+([^\s(]+)",
            "CREATE TRIGGER": r"CREATE\s+TRIGGER\s+([^\s;]+)",
            "DROP TRIGGER": r"DROP\s+TRIGGER\s+([^\s;]+)",
            "CREATE DATABASE": r"CREATE\s+DATABASE\s+([^\s;]+)",
            "DROP DATABASE": r"DROP\s+DATABASE\s+([^\s;]+)",
            "CREATE SCHEMA": r"CREATE\s+SCHEMA\s+([^\s;]+)",
            "DROP SCHEMA": r"DROP\s+SCHEMA\s+([^\s;]+)",
            "INSERT": r"INSERT\s+INTO\s+([^\s(]+)",
            "UPDATE": r"UPDATE\s+([^\s;]+)",
            "DELETE": r"DELETE\s+FROM\s+([^\s;]+)",
            "MERGE": r"MERGE\s+INTO\s+([^\s;]+)",
            "SELECT": r"FROM\s+([^\s,;]+)",
            "GRANT": r"GRANT\s+[^\s]+\s+ON\s+([^\s;]+)",
            "REVOKE": r"REVOKE\s+[^\s]+\s+ON\s+([^\s;]+)",
            "DESCRIBE": r"DESCRIBE\s+([^\s;]+)",
            "EXPLAIN": r"EXPLAIN\s+([^\s;]+)",
            "EXPLAIN ANALYZE": r"EXPLAIN\s+ANALYZE\s+([^\s;]+)",
            "SHOW": r"SHOW\s+([^;]+)",
            "USE": r"USE\s+([^;]+)",
        }
        statement_upper = self._normalize(statement)
        results: List[str] = []
        for key, pattern in patterns.items():
            if action == key or (key == "SELECT" and "SELECT" in statement_upper):
                matches = re.findall(pattern, statement_upper, flags=re.IGNORECASE)
                results.extend(matches)
        return list(dict.fromkeys(results))

    def _detect_clauses(self, statement: str) -> List[str]:
        normalized = self._normalize(statement)
        found = [clause for clause in self.CLAUSES if clause in normalized]
        return sorted(found)

    def _detect_functions(self, statement: str) -> List[str]:
        normalized = self._normalize(statement)
        found: List[str] = []
        for func in self.FUNCTIONS:
            if func in {"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP"}:
                if func in normalized:
                    found.append(func)
            else:
                if re.search(rf"\b{re.escape(func)}\s*\(", normalized):
                    found.append(func)
        return sorted(set(found))

    @staticmethod
    def _normalize(statement: str) -> str:
        return re.sub(r"\s+", " ", statement.strip()).upper()
