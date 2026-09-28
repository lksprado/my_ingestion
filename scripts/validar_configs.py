"""Valida a estrutura dos ``<fonte>_config.yml`` (hook do pre-commit).

Uso:
    uv run python scripts/validar_configs.py [arquivo ...]

Sem argumentos valida todos os ``src/pipelines/**/*_config.yml``. Recebe também
``<fonte>_etl.py`` (valida o YAML da mesma pasta). YAML sem ``sources:`` não é
config de pipeline (ex.: ``precos/atacadao``) e é pulado.

Além de ``core.validate_config``, confere que as sources do YAML são as chaves de
``ETLS`` no ``<fonte>_etl.py`` da pasta (ou são pedidas por um
``PipelineConfig.from_yaml(..., "<source>")`` literal num script da pasta), e
vice-versa. Tudo lido com ``ast``, sem importar os módulos (importar puxaria
dependências do pipeline).
"""

import ast
import sys
from pathlib import Path

from core.config import load_yaml, validate_config

REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINES = REPO_ROOT / "src" / "pipelines"


def etls_keys(folder: Path) -> set[str] | None:
    """Chaves literais do dict ``ETLS`` do ``*_etl.py`` da pasta (None se não há)."""
    for etl_file in sorted(folder.glob("*_etl.py")):
        tree = ast.parse(etl_file.read_text(encoding="utf-8"))
        for node in tree.body:
            if (
                isinstance(node, ast.Assign)
                and any(
                    isinstance(t, ast.Name) and t.id == "ETLS" for t in node.targets
                )
                and isinstance(node.value, ast.Dict)
                and all(isinstance(k, ast.Constant) for k in node.value.keys)
            ):
                return {k.value for k in node.value.keys}
    return None


def from_yaml_sources(folder: Path) -> set[str]:
    """Sources pedidas literalmente a ``from_yaml`` pelos scripts da pasta.

    Cobre as exceções ao ``GenericETL`` que leem uma source do mesmo YAML sem
    passar pelo ``ETLS`` (ex.: ``investimentos_fgc.py`` -> ``fgc``).
    """
    found: set[str] = set()
    for py in folder.glob("*.py"):
        for node in ast.walk(ast.parse(py.read_text(encoding="utf-8"))):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "from_yaml"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
            ):
                found.add(node.args[1].value)
    return found


def validate_file(path: Path) -> list[str]:
    try:
        data = load_yaml(path)
    except Exception as e:  # sintaxe já é do check-yaml; aqui só não quebra
        return [f"não foi possível ler o YAML: {e}"]
    if not isinstance(data, dict) or "sources" not in data:
        return []

    errors = validate_config(data)
    sources = data["sources"]
    keys = etls_keys(path.parent)
    if keys is not None and isinstance(sources, dict):
        for name in sorted(set(sources) - keys - from_yaml_sources(path.parent)):
            errors.append(f"sources.{name}: sem entrada em ETLS do *_etl.py")
        for name in sorted(keys - set(sources)):
            errors.append(f"ETLS[{name!r}]: sem source correspondente no YAML")
    return errors


def targets(args: list[str]) -> list[Path]:
    if not args:
        return sorted(PIPELINES.rglob("*_config.yml"))
    paths: set[Path] = set()
    for arg in args:
        p = Path(arg).resolve()
        if p.name.endswith("_config.yml"):
            paths.add(p)
        elif p.name.endswith("_etl.py"):
            paths.update(p.parent.glob("*_config.yml"))
    return sorted(paths)


def main(argv: list[str]) -> int:
    failed = False
    for path in targets(argv):
        errors = validate_file(path)
        rel = path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path
        for err in errors:
            print(f"{rel}: {err}")
        failed |= bool(errors)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
