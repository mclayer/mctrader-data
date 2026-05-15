"""MCT-182 §8 TDD — data shim backward-compat (INV-4).

INV-4: data shim는 mctrader_market 의 동일 객체를 re-export (is 동일성 보장).
       DeprecationWarning 발화 확인.
INV-6: tick_storage / orderbook_storage 의 pyarrow Writer 는 무변경.
"""

from __future__ import annotations

import warnings


# ---------------------------------------------------------------------------
# INV-4: aggregation shim — is 동일성 (copy 아닌 re-export)
# ---------------------------------------------------------------------------

class TestAggregationShimIdentity:
    """INV-4 — mctrader_data.aggregation shim: market 객체와 is 동일."""

    def test_contract_metadata_is_same_class(self) -> None:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from mctrader_data.aggregation import ContractMetadata as DataCM
        from mctrader_market.aggregation import ContractMetadata as MarketCM
        assert DataCM is MarketCM

    def test_compute_contract_id_is_same_function(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from mctrader_data.aggregation import compute_contract_id as data_fn
        from mctrader_market.aggregation import compute_contract_id as market_fn
        assert data_fn is market_fn

    def test_to_scaled_is_same_function(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from mctrader_data.aggregation import to_scaled as data_fn
        from mctrader_market.aggregation import to_scaled as market_fn
        assert data_fn is market_fn

    def test_from_scaled_is_same_function(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from mctrader_data.aggregation import from_scaled as data_fn
        from mctrader_market.aggregation import from_scaled as market_fn
        assert data_fn is market_fn

    def test_tick_bar_aggregator_is_same_class(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from mctrader_data.aggregation import TickBarAggregator as DataAgg
        from mctrader_market.aggregation import TickBarAggregator as MarketAgg
        assert DataAgg is MarketAgg

    def test_volume_bar_aggregator_is_same_class(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from mctrader_data.aggregation import VolumeBarAggregator as DataAgg
        from mctrader_market.aggregation import VolumeBarAggregator as MarketAgg
        assert DataAgg is MarketAgg

    def test_time_bar_aggregator_is_same_class(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from mctrader_data.aggregation import TimeBarAggregator as DataAgg
        from mctrader_market.aggregation import TimeBarAggregator as MarketAgg
        assert DataAgg is MarketAgg

    def test_dollar_bar_aggregator_is_same_class(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from mctrader_data.aggregation import DollarBarAggregator as DataAgg
        from mctrader_market.aggregation import DollarBarAggregator as MarketAgg
        assert DataAgg is MarketAgg


# ---------------------------------------------------------------------------
# INV-4: aggregation shim DeprecationWarning 발화 확인
# ---------------------------------------------------------------------------

class TestAggregationShimDeprecationWarning:
    """INV-4 — import mctrader_data.aggregation → DeprecationWarning 발화."""

    def test_import_raises_deprecation_warning(self) -> None:
        import sys
        # Remove cached module to force re-import with warning
        for key in list(sys.modules.keys()):
            if key == "mctrader_data.aggregation":
                del sys.modules[key]
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            import mctrader_data.aggregation  # noqa: F401
            assert len(w) >= 1
            categories = [warning.category for warning in w]
            assert DeprecationWarning in categories, (
                f"Expected DeprecationWarning, got: {categories}"
            )


# ---------------------------------------------------------------------------
# INV-4: paper_lineage shim — is 동일성
# ---------------------------------------------------------------------------

class TestPaperLineageShimIdentity:
    """INV-4 — mctrader_data.paper_lineage shim: market 객체와 is 동일."""

    def test_paper_lineage_is_same_class(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from mctrader_data.paper_lineage import PaperLineage as DataPL
        from mctrader_market.paper_lineage import PaperLineage as MarketPL
        assert DataPL is MarketPL

    def test_canonical_jsonl_hash_is_same_function(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from mctrader_data.paper_lineage import canonical_jsonl_hash as data_fn
        from mctrader_market.paper_lineage import canonical_jsonl_hash as market_fn
        assert data_fn is market_fn

    def test_paper_lineage_import_raises_deprecation_warning(self) -> None:
        import sys
        for key in list(sys.modules.keys()):
            if key == "mctrader_data.paper_lineage":
                del sys.modules[key]
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            import mctrader_data.paper_lineage  # noqa: F401
            assert len(w) >= 1
            categories = [warning.category for warning in w]
            assert DeprecationWarning in categories, (
                f"Expected DeprecationWarning, got: {categories}"
            )


# ---------------------------------------------------------------------------
# INV-4: tick_storage / orderbook_storage import mctrader_market.records
# ---------------------------------------------------------------------------

class TestStorageImportsMarketRecords:
    """INV-4 — tick_storage / orderbook_storage 는 market.records 에서 import."""

    def test_tick_record_in_tick_storage_is_market_record(self) -> None:
        from mctrader_data.tick_storage import TickRecord as DataTR
        from mctrader_market.records import TickRecord as MarketTR
        assert DataTR is MarketTR

    def test_orderbook_record_in_orderbook_storage_is_market_record(self) -> None:
        from mctrader_data.orderbook_storage import OrderbookEventRecord as DataOR
        from mctrader_market.records import OrderbookEventRecord as MarketOR
        assert DataOR is MarketOR


# ---------------------------------------------------------------------------
# INV-6: pyarrow Writer 무변경 (tick_storage / orderbook_storage 에 잔류)
# ---------------------------------------------------------------------------

class TestPyarrowWriterUnchanged:
    """INV-6 — TickWriter / OrderbookWriter 는 data 에 잔류 (pyarrow 결합 유지)."""

    def test_tick_writer_importable_from_data(self) -> None:
        import importlib
        mod = importlib.import_module("mctrader_data.tick_storage")
        assert hasattr(mod, "TickWriter"), "TickWriter must remain in mctrader_data.tick_storage"

    def test_orderbook_writer_importable_from_data(self) -> None:
        import importlib
        mod = importlib.import_module("mctrader_data.orderbook_storage")
        assert hasattr(mod, "OrderbookWriter"), (
            "OrderbookWriter must remain in mctrader_data.orderbook_storage"
        )

    def test_tick_storage_still_has_pyarrow_dep(self) -> None:
        """tick_storage.py 는 여전히 pyarrow 를 import해야 함 (Writer 경로)."""
        import ast
        import inspect
        import mctrader_data.tick_storage as mod
        src = inspect.getsource(mod)
        tree = ast.parse(src)
        pyarrow_imported = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("pyarrow"):
                        pyarrow_imported = True
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("pyarrow"):
                    pyarrow_imported = True
        assert pyarrow_imported, "tick_storage.py must still import pyarrow (Writer 잔류, INV-6)"

    def test_storage_uses_market_records_not_local_dataclass(self) -> None:
        """tick_storage imports TickRecord from mctrader_market.records (not local def)."""
        import ast
        import inspect
        import mctrader_data.tick_storage as mod
        src = inspect.getsource(mod)
        tree = ast.parse(src)
        imports_market_tick = False
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if "mctrader_market" in module and "records" in module:
                    imports_market_tick = True
        assert imports_market_tick, (
            "tick_storage.py must import TickRecord from mctrader_market.records"
        )


# ---------------------------------------------------------------------------
# INV-4: cold path SSOT — duckdb_resample / polars_fallback 검증 사각지대 해소
# MCT-182 fix1 (CodeReview P1 F-2 resolution — ArchitectPL Option A 채택)
# ---------------------------------------------------------------------------

class TestColdPathUsesMctraderMarketSot:
    """INV-4 — cold path 가 import 하는 Aggregator 가 market SSOT 와 is 동일.

    duckdb_resample.py / polars_fallback.py 는 aggregation.__init__ shim 을
    우회하고 aggregation.core 를 직접 import 할 수 있다. 해당 경로에서 얻은
    Aggregator 클래스가 mctrader_market.aggregation 의 동일 객체인지 검증.
    (Change Plan §8 INV-4 보강 명세 — ArchitectPL 판정 §4.2 정정 정합)
    """

    def test_duckdb_resample_dollar_bar_aggregator_is_market_sot(self) -> None:
        """duckdb_resample 에서 사용하는 DollarBarAggregator is market SSOT."""
        import ast
        import inspect
        import mctrader_data.cold.duckdb_resample as cold_mod
        from mctrader_market.aggregation import DollarBarAggregator as MarketDollar

        # AST 로 import 출처 검증 — market 에서 오는지 확인
        src = inspect.getsource(cold_mod)
        tree = ast.parse(src)
        imports_data_core = False
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if "mctrader_data.aggregation.core" in module:
                    imports_data_core = True
        assert not imports_data_core, (
            "duckdb_resample.py must NOT import from mctrader_data.aggregation.core "
            "(shim bypass — use mctrader_market.aggregation directly, INV-4)"
        )

        # 런타임 is-동일성 — cold mod 의 DollarBarAggregator 가 market SSOT
        cold_dollar = getattr(cold_mod, "DollarBarAggregator", None)
        assert cold_dollar is not None, "DollarBarAggregator not found in duckdb_resample"
        assert cold_dollar is MarketDollar, (
            "duckdb_resample.DollarBarAggregator is not mctrader_market.aggregation.DollarBarAggregator "
            "(SSOT 이중화 — INV-4 위반)"
        )

    def test_duckdb_resample_tick_bar_aggregator_is_market_sot(self) -> None:
        """duckdb_resample 에서 사용하는 TickBarAggregator is market SSOT."""
        import mctrader_data.cold.duckdb_resample as cold_mod
        from mctrader_market.aggregation import TickBarAggregator as MarketTick
        cold_tick = getattr(cold_mod, "TickBarAggregator", None)
        assert cold_tick is not None, "TickBarAggregator not found in duckdb_resample"
        assert cold_tick is MarketTick, (
            "duckdb_resample.TickBarAggregator is not mctrader_market.aggregation.TickBarAggregator (INV-4)"
        )

    def test_duckdb_resample_time_bar_aggregator_is_market_sot(self) -> None:
        """duckdb_resample 에서 사용하는 TimeBarAggregator is market SSOT."""
        import mctrader_data.cold.duckdb_resample as cold_mod
        from mctrader_market.aggregation import TimeBarAggregator as MarketTime
        cold_time = getattr(cold_mod, "TimeBarAggregator", None)
        assert cold_time is not None, "TimeBarAggregator not found in duckdb_resample"
        assert cold_time is MarketTime, (
            "duckdb_resample.TimeBarAggregator is not mctrader_market.aggregation.TimeBarAggregator (INV-4)"
        )

    def test_duckdb_resample_volume_bar_aggregator_is_market_sot(self) -> None:
        """duckdb_resample 에서 사용하는 VolumeBarAggregator is market SSOT."""
        import mctrader_data.cold.duckdb_resample as cold_mod
        from mctrader_market.aggregation import VolumeBarAggregator as MarketVolume
        cold_volume = getattr(cold_mod, "VolumeBarAggregator", None)
        assert cold_volume is not None, "VolumeBarAggregator not found in duckdb_resample"
        assert cold_volume is MarketVolume, (
            "duckdb_resample.VolumeBarAggregator is not mctrader_market.aggregation.VolumeBarAggregator (INV-4)"
        )

    def test_polars_fallback_time_bar_aggregator_is_market_sot(self) -> None:
        """polars_fallback 에서 사용하는 TimeBarAggregator is market SSOT."""
        import ast
        import inspect
        import mctrader_data.cold.polars_fallback as fallback_mod
        from mctrader_market.aggregation import TimeBarAggregator as MarketTime

        # AST 로 import 출처 검증
        src = inspect.getsource(fallback_mod)
        tree = ast.parse(src)
        imports_data_core = False
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if "mctrader_data.aggregation.core" in module:
                    imports_data_core = True
        assert not imports_data_core, (
            "polars_fallback.py must NOT import from mctrader_data.aggregation.core "
            "(shim bypass — use mctrader_market.aggregation directly, INV-4)"
        )

        # 런타임 is-동일성
        fallback_time = getattr(fallback_mod, "TimeBarAggregator", None)
        assert fallback_time is not None, "TimeBarAggregator not found in polars_fallback"
        assert fallback_time is MarketTime, (
            "polars_fallback.TimeBarAggregator is not mctrader_market.aggregation.TimeBarAggregator (INV-4)"
        )
