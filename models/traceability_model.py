import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class TraceabilityRow:
    source_schema: Optional[str]
    source_table: Optional[str]
    source_field: Optional[str]
    destination_schema: Optional[str]
    destination_table: Optional[str]
    destination_field: Optional[str]
    logic: str
    filter: Optional[str]


class TraceabilityModel:
    DEST_PATTERNS = (
        r"INSERT\s+INTO\s+(?P<dest>[^\s(]+)\s*(?:\((?P<dest_cols>[^)]*)\))?",
        r"CREATE\s+TABLE\s+(?P<dest>[^\s(]+).*?\s+AS",
        r"CREATE\s+VIEW\s+(?P<dest>[^\s(]+).*?\s+AS",
    )

    def analyze(self, statement: str) -> List[Dict[str, object]]:
        select_info = self._extract_select(statement)
        if not select_info:
            return []

        select_list, from_section, where_clause = select_info
        alias_map, sources = self._extract_sources(from_section)
        dest_table, dest_columns = self._extract_destination(statement)
        dest_schema, dest_table_name = self._split_identifier(dest_table) if dest_table else (None, None)

        rows: List[TraceabilityRow] = []
        select_items = self._split_select_items(select_list)
        for index, item in enumerate(select_items):
            expression, alias = self._split_alias(item)
            source_schema, source_table, source_field = self._resolve_source(
                expression,
                alias_map,
                sources,
            )
            destination_field = None
            if dest_columns and index < len(dest_columns):
                destination_field = dest_columns[index]
            elif alias:
                destination_field = alias
            else:
                destination_field = source_field

            rows.append(
                TraceabilityRow(
                    source_schema=source_schema,
                    source_table=source_table,
                    source_field=source_field,
                    destination_schema=dest_schema,
                    destination_table=dest_table_name,
                    destination_field=destination_field,
                    logic=expression.strip(),
                    filter=where_clause,
                )
            )

        return [row.__dict__ for row in rows]

    def _extract_destination(self, statement: str) -> Tuple[Optional[str], Optional[List[str]]]:
        for pattern in self.DEST_PATTERNS:
            match = re.search(pattern, statement, flags=re.IGNORECASE | re.DOTALL)
            if match:
                dest = match.group("dest")
                dest_cols = match.groupdict().get("dest_cols")
                columns = None
                if dest_cols:
                    columns = [col.strip() for col in dest_cols.split(",") if col.strip()]
                return dest, columns
        return None, None

    def _extract_select(self, statement: str) -> Optional[Tuple[str, str, Optional[str]]]:
        match = re.search(
            r"SELECT\s+(?P<select>.+?)\s+FROM\s+(?P<from>.+?)(?:\s+WHERE\s+(?P<where>.+?))?(?:\s+GROUP\s+BY|\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|\s+OFFSET|;|$)",
            statement,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return None
        select_list = match.group("select").strip()
        from_section = match.group("from").strip()
        where_clause = match.group("where")
        if where_clause:
            where_clause = where_clause.strip()
        return select_list, from_section, where_clause

    def _extract_sources(self, from_section: str) -> Tuple[Dict[str, str], List[str]]:
        alias_map: Dict[str, str] = {}
        sources: List[str] = []
        for table, alias in re.findall(
            r"(?:FROM|JOIN)\s+([^\s,]+)(?:\s+(?:AS\s+)?([^\s,]+))?",
            from_section,
            flags=re.IGNORECASE,
        ):
            sources.append(table)
            if alias:
                alias_map[alias] = table
        return alias_map, sources

    def _split_select_items(self, select_list: str) -> List[str]:
        items: List[str] = []
        current: List[str] = []
        depth = 0
        for char in select_list:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            if char == "," and depth == 0:
                item = "".join(current).strip()
                if item:
                    items.append(item)
                current = []
            else:
                current.append(char)
        if current:
            item = "".join(current).strip()
            if item:
                items.append(item)
        return items

    def _split_alias(self, item: str) -> Tuple[str, Optional[str]]:
        match = re.search(r"\s+AS\s+([^\s]+)$", item, flags=re.IGNORECASE)
        if match:
            alias = match.group(1).strip()
            expression = item[: match.start()].strip()
            return expression, alias
        parts = item.rsplit(" ", 1)
        if len(parts) == 2 and "(" not in parts[1]:
            return parts[0], parts[1]
        return item, None

    def _resolve_source(
        self,
        expression: str,
        alias_map: Dict[str, str],
        sources: List[str],
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        schema_table_column = re.search(
            r"([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)",
            expression,
        )
        if schema_table_column:
            schema, table, column = schema_table_column.groups()
            return schema, table, column

        table_column = re.search(r"([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)", expression)
        if table_column:
            qualifier, column = table_column.groups()
            table_ref = alias_map.get(qualifier, qualifier)
            schema, table = self._split_identifier(table_ref)
            return schema, table, column

        if sources:
            schema, table = self._split_identifier(sources[0])
            column_match = re.search(r"\b([A-Za-z_][\w]*)\b", expression)
            column = column_match.group(1) if column_match else None
            return schema, table, column

        return None, None, None

    @staticmethod
    def _split_identifier(identifier: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not identifier:
            return None, None
        parts = identifier.split(".")
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, parts[0]
