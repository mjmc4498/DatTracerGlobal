import re
from typing import Dict, List, Set


class LineageModel:
    def analyze(self, statement: str, action: str, objects: List[str]) -> Dict[str, object]:
        normalized = self._normalize(statement)
        sources = self._extract_sources(normalized)
        targets: List[str] = []

        if action in {"CREATE VIEW", "CREATE TABLE", "INSERT", "MERGE"}:
            if objects:
                targets.append(objects[0])
        elif action == "UPDATE" and objects:
            targets.append(objects[0])

        edges: List[Dict[str, str]] = []
        nodes: Set[str] = set()

        for target in targets:
            nodes.add(target)
            for source in sources:
                if source != target:
                    nodes.add(source)
                    edges.append(
                        {
                            "from": source,
                            "to": target,
                            "relation": "lineage",
                        }
                    )

        if nodes or edges:
            return {"nodes": nodes, "edges": edges}
        return {}

    def _extract_sources(self, normalized: str) -> List[str]:
        sources: List[str] = []
        from_matches = re.findall(r"\bFROM\s+([^\s,;]+)", normalized, flags=re.IGNORECASE)
        join_matches = re.findall(r"\bJOIN\s+([^\s,;]+)", normalized, flags=re.IGNORECASE)
        using_matches = re.findall(r"\bUSING\s+([^\s,;]+)", normalized, flags=re.IGNORECASE)
        sources.extend(from_matches + join_matches + using_matches)
        return list(dict.fromkeys(sources))

    @staticmethod
    def _normalize(statement: str) -> str:
        return re.sub(r"\s+", " ", statement.strip()).upper()
