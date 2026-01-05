import re
from typing import Any, Callable, Optional
from unittest.mock import MagicMock

import jinja2


def test_partitioned_table_macro_renders_partitions() -> None:
    """
    Unit test for the 'partitioned_table' macro using Jinja2 rendering.
    Verifies that the macro generates explicit partition creation statements.
    """

    # 1. Load the macro content
    macro_path = "dbt_pubmed/macros/postgres_partitioned_table.sql"
    with open(macro_path, "r") as f:
        macro_content = f.read()

    # 2. Extract the body of the materialization
    match = re.search(
        r"{% materialization partitioned_table, adapter='postgres' %}(.*){% endmaterialization %}",
        macro_content,
        re.DOTALL,
    )
    assert match, "Could not find materialization block in macro file"
    template_body = match.group(1)

    # 3. Setup Jinja Environment and Mocks
    env = jinja2.Environment()

    # Mock Context Objects
    mock_this = MagicMock()
    # Mocking basic dbt Relation behavior
    mock_this.identifier = "gold_table"
    mock_this.__str__.return_value = '"my_schema"."gold_table"'  # type: ignore[attr-defined]

    def mock_incorporate(path: Any) -> Any:
        # Simulate creating a new relation with updated identifier
        new_identifier = path.get("identifier")
        new_mock = MagicMock()
        # dbt quotes the identifier in __str__
        new_mock.__str__.return_value = f'"my_schema"."{new_identifier}"'  # type: ignore[attr-defined]
        return new_mock

    mock_this.incorporate.side_effect = mock_incorporate

    mock_config = MagicMock()
    mock_config.get.return_value = "publication_year"  # partition_by

    mock_adapter = MagicMock()

    def mock_statement(name: str, caller: Optional[Callable[[], str]] = None) -> str:
        if caller:
            return caller()
        return ""

    # Mock global functions available in dbt jinja context
    context = {
        "this": mock_this,
        "config": mock_config,
        "adapter": mock_adapter,
        "model": MagicMock(name="gold_pubmed_knowledge"),
        "exceptions": MagicMock(),
        "run_hooks": MagicMock(return_value="-- hooks run"),
        "load_relation": MagicMock(return_value=None),
        "make_temp_relation": MagicMock(return_value='"my_schema"."gold_table__dbt_tmp"'),
        "create_table_as": MagicMock(return_value="CREATE TABLE tmp AS SELECT ..."),
        "create_indexes": MagicMock(return_value="-- indexes created"),
        "sql": "SELECT * FROM ...",
        "statement": mock_statement,
        "return": MagicMock(return_value=""),  # Mock 'return' function
    }

    # 4. Render
    template = env.from_string(template_body)
    rendered = template.render(**context)

    # 5. Assertions
    print(rendered)

    # Verify basic structure
    assert 'CREATE TABLE "my_schema"."gold_table"' in rendered
    assert "PARTITION BY RANGE (publication_year)" in rendered

    # Verify Default Partition (Correctly quoted)
    assert 'CREATE TABLE IF NOT EXISTS "my_schema"."gold_table_default"' in rendered
    assert 'PARTITION OF "my_schema"."gold_table" DEFAULT' in rendered

    # Verify Year Partitions
    # We check that the specific partition table is created with correct quoting
    assert 'CREATE TABLE IF NOT EXISTS "my_schema"."gold_table_2024"' in rendered

    # Check bounds (robust to whitespace by splitting)
    cleaned_rendered = re.sub(r"\s+", " ", rendered)

    assert (
        'PARTITION OF "my_schema"."gold_table" FOR VALUES FROM (2024) TO (2025)' in cleaned_rendered
    ), "Partition bounds for year 2024 not found"
    assert (
        'PARTITION OF "my_schema"."gold_table" FOR VALUES FROM (1900) TO (1901)' in cleaned_rendered
    ), "Partition bounds for year 1900 not found"


if __name__ == "__main__":
    test_partitioned_table_macro_renders_partitions()
