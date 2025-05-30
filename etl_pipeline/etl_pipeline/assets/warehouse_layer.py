from dagster import multi_asset, Output, AssetIn, AssetOut, asset
import pandas as pd

@multi_asset(
    ins={
        "gold_statsPerLeagueSeason": AssetIn(
            key_prefix=["football", "gold"]
        )
    },
    outs={
        "statsperleagueseason": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["statsPerLeagueSeason", 'analysis'],
            metadata={
                "columns": [
                    "name",
                    "season",
                    "goals",
                    "xGoals",
                    "shots",
                    "shotsOnTarget",
                    "fouls",
                    "yellowCards",
                    "redCards",
                    "corners",
                    "games",
                    "goalPerGame"
                ]
            }
        ),
    },
    compute_kind="PostgreSQL",
    group_name="Warehouse_layer"
)
def statsPerLeagueSeason(gold_statsPerLeagueSeason: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        gold_statsPerLeagueSeason,
        metadata={
            "schema": "analysis",
            "table": "statsPerLeagueSeason",
            "records": len(gold_statsPerLeagueSeason)
        }
    )


@multi_asset(
    ins={
        "gold_statsPerPlayerSeason": AssetIn(
            key_prefix=['football', 'gold']
        )
    },
    outs={
        "statsperplayerseason": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["statsPerPlayerSeason", 'analysis'],
            metadata={
                "columns": [
                    "playerID",
                    "name",
                    "season",
                    "goals",
                    "shots",
                    "xGoals",
                    "xGoalsChain",
                    "xGoalsBuildup",
                    "assists",
                    "keyPasses",
                    "xAssists",
                    "gDiff",
                    "gDiffRatio"
                ]
            }
        )
    },
    compute_kind="PostgreSQL",
    group_name="Warehouse_layer"
)
def statsPerPlayerSeason(gold_statsPerPlayerSeason: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        gold_statsPerPlayerSeason,
        metadata={
            "schema": "analysis",
            "table": "statsPerPlayerSeason",
            "records": len(gold_statsPerPlayerSeason)
        }
    )


@multi_asset(
    ins={
        "gold_statsPlayerPer90": AssetIn(
            key_prefix=['football', 'gold']
        )
    },
    outs={
        "statsplayerper90": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["statsPlayerPer90", 'analysis'],
            metadata={
                "columns": [
                    'playerID',
                    'name',
                    'total_goals',
                    'total_assists',
                    'total_time',
                    'goalsPer90',
                    'assistsPer90',
                    'scorers'
                ]
            }
        )
    },
    compute_kind="PostgreSQL",
    group_name="Warehouse_layer"
)
def statsPlayerPer90(gold_statsPlayerPer90: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        gold_statsPlayerPer90,
        metadata={
            "schema": "analysis",
            "table": "statsPlayerPer90",
            "records": len(gold_statsPlayerPer90)
        }
    )