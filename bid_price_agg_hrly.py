import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from datetime import datetime
from datetime import timedelta
from typing import Dict
from typing import List

import boto3
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.connect.functions import explode
from pyspark.sql.types import ArrayType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import MapType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class BidderLogAggregationHour:
    def __init__(
        self,
        env: str,
        data_source_date: str,
        data_source_hour: None,
        cgid_mods: List[int],
        log_level: int = logging.INFO,
    ):
        self._env = env
        self._data_source_date = data_source_date
        self._data_source_hour = data_source_hour
        self._cgid_mods = cgid_mods
        self._log_level = log_level

    @property
    def env(self):  # type: ignore[no-untyped-def]
        return self._env

    @property
    def data_source_date(self):  # type: ignore[no-untyped-def]
        return self._data_source_date

    @property
    def get_cgid_mods(self):  # type: ignore[no-untyped-def]
        return self._cgid_mods

    @property
    def data_source_hour(self):  # type: ignore[no-untyped-def]
        if self._data_source_hour is None or self._data_source_hour == "None":
            return None
        return self._data_source_hour

    @property
    def s3_path_out(self):  # type: ignore[no-untyped-def]
        S3Path = f"s3://mntn-data-archive-{self.env}/bid_price_log_agg_v2/"
        return S3Path

    @property
    def log_level(self) -> int:
        return self._log_level

    @log_level.setter
    def log_level(self, value):  # type: ignore[no-untyped-def]
        self._log_level = value if value else logging.INFO
        logger.setLevel(self.log_level)

    @property
    def s3_client(self):  # type: ignore[no-untyped-def]
        return boto3.client("s3", "us-west-2")

    @property
    def data_source_bucket(self) -> str:
        return f"mntn-data-archive-{self.env}"

    @property
    def base_path(self) -> str:
        return f"s3://{self.data_source_bucket}/bid_price_log_v2"

    @property
    def data_source_athena_table(self) -> str:
        return f"data_archive_{self.env}.bid_price_logs_aggregated_hour"

    @property
    def data_source_prefix(self) -> list:
        hours = range(24)
        if 999 in self.get_cgid_mods:
            if self.data_source_hour is None or self.data_source_hour in "":
                return [f"dt={self.data_source_date}/hh={hour:02d}" for hour in hours]
            return [f"dt={self.data_source_date}/hh={self.data_source_hour}"]
        else:
            if self.data_source_hour is None or self.data_source_hour in "":
                return [
                    f"dt={self.data_source_date}/hh={hour:02d}/cgid_mod={mod}"
                    for hour in hours
                    for mod in self.get_cgid_mods
                ]
            return [
                f"dt={self.data_source_date}/hh={self.data_source_hour}/cgid_mod={mod}"
                for mod in self.get_cgid_mods
            ]

    @property
    def s3_path(self) -> list:
        return [f"{self.base_path}/{path}" for path in self.data_source_prefix]

    @property
    def partition_columns(self) -> List[str]:
        return [
            "dt",
            "hh",
            "cgid_mod",
        ]

    @property
    def spark(self) -> SparkSession:
        _spark = (
            SparkSession.builder.appName(
                f"Populate aggregated data for bidder for date {self.data_source_date}"
            )
            .enableHiveSupport()
            .config("spark.sql.hive.convertMetastoreParquet", "false")
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
            .config("spark.driver.maxResultSize", "32G")
            .config("spark.sql.shuffle.partitions", "10000")
            .config("spark.driver.memoryOverhead", "3277m")
            .config("spark.executor.memoryOverhead", "3277m")
            .config("spark.executor.instances", "50")
            .config("spark.num.executors", "50")
            .config("spark.executor.memory", "32G")
            .config("spark.executor.cores", "5")
            .config("spark.driver.cores", "5")
            .config("spark.driver.memory", "32G")
            .config("spark.memory.fraction", "0.8")
            .config("spark.speculation", "true")
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.yarn.heterogeneousExecutors.enabled", "false")
            .config("spark.emr.default.executor.cores", "5")
            .config("spark.emr.default.executor.memory", "32G")
            .config("spark.sql.jsonGenerator.ignoreNullFields", "false")
            .getOrCreate()
        )
        return _spark

    def s3_path_exists(self, bucket: str, prefix: str) -> bool:
        s3_objs = self.s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1,
        )
        return bool(s3_objs["KeyCount"] > 0)

    @property
    def nested_schema(self) -> StructType:
        return StructType(
            [
                StructField("flight_campaign_spend", StringType(), True),
                StructField("flight_campaign_spend_cap", StringType(), True),
                StructField("flight_campaign_group_spend", StringType(), True),
                StructField("flight_campaign_group_spend_cap", StringType(), True),
                StructField("flight_campaign_impression", StringType(), True),
                StructField("flight_campaign_impression_cap", StringType(), True),
                StructField("flight_campaign_group_impression", StringType(), True),
                StructField("flight_campaign_group_impression_cap", StringType(), True),
                StructField("daily_campaign_group_spend", StringType(), True),
                StructField("daily_campaign_group_spend_cap", StringType(), True),
                StructField("daily_campaign_spend", StringType(), True),
                StructField("daily_campaign_spend_cap", StringType(), True),
                StructField("daily_campaign_impression", StringType(), True),
                StructField("daily_campaign_impression_cap", StringType(), True),
                StructField("daily_campaign_group_impression", StringType(), True),
                StructField("daily_campaign_group_impression_cap", StringType(), True),
                StructField("picked_term_id", StringType(), True),
                StructField("eligible_term_ids", StringType(), True),
                StructField("terms", ArrayType(MapType(StringType(), StringType())), True),
            ]
        )

    def path_exists(self, s3_uri):  # type: ignore[no-untyped-def]
        bucket, key = s3_uri.replace("s3://", "").split("/", 1)
        return self.s3_path_exists(bucket, key)

    def _get_bidder_logs_df(self) -> Dict[str, List[DataFrame]]:
        logger.log(self.log_level, "Begin '_get_bidder_logs_df'")
        logger.log(self.log_level, "Begin '_bid_price_log_aggregation'")
        valid_paths = [path for path in self.s3_path if self.path_exists(path)]
        missed_files = [missed for missed in self.s3_path if not self.path_exists(missed)]
        # df_bid_select =
        for paths in valid_paths:
            print(paths)
        logger.info(f"following partitions are missing in S3 {missed_files}")
        # if valid_paths:
        df_bid_select = self.spark.read.option("basePath", self.base_path).parquet(*valid_paths)

        F.from_json(F.col("pacing_debug_data"), self.nested_schema).getField("terms")
        df_columns_selected = df_bid_select.select(
            F.col("epoch"),
            F.col("advertiser_id"),
            F.col("campaign_group_id"),
            F.col("campaign_id"),
            F.col("flight_id"),
            F.col("auction_id"),
            F.col("has_price").cast("boolean"),
            F.col("price"),
            F.col("threshold_failure_reasons").alias("threshold_failure_reasons"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("terms")
            .alias("terms"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("picked_term_id")
            .alias("picked_term_id"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("eligible_term_ids")
            .alias("eligible_term_id"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("flight_campaign_spend")
            .alias("flight_campaign_spend"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("flight_campaign_spend_cap")
            .alias("flight_campaign_spend_cap"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("flight_campaign_group_spend")
            .alias("flight_campaign_group_spend"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("flight_campaign_group_spend_cap")
            .alias("flight_campaign_group_spend_cap"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("flight_campaign_impression")
            .alias("flight_campaign_impression"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("flight_campaign_impression_cap")
            .alias("flight_campaign_impression_cap"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("flight_campaign_group_impression")
            .alias("flight_campaign_group_impression"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("flight_campaign_group_impression_cap")
            .alias("flight_campaign_group_impression_cap"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("daily_campaign_group_spend")
            .alias("daily_campaign_group_spend"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("daily_campaign_group_spend_cap")
            .alias("daily_campaign_group_spend_cap"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("daily_campaign_spend")
            .alias("daily_campaign_spend"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("daily_campaign_spend_cap")
            .alias("daily_campaign_spend_cap"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("daily_campaign_impression")
            .alias("daily_campaign_impression"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("daily_campaign_impression_cap")
            .alias("daily_campaign_impression_cap"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("daily_campaign_group_impression")
            .alias("daily_campaign_group_impression"),
            F.from_json(F.col("pacing_debug_data"), self.nested_schema)
            .getField("daily_campaign_group_impression_cap")
            .alias("daily_campaign_group_impression_cap"),
            (F.col("campaign_group_id") % 100).alias("cgid_mod"),
            (F.date_format(F.from_unixtime(F.col("epoch") / 1000), "yyyy-MM-dd")).alias("dt"),
            (F.hour(F.from_unixtime(F.col("epoch") / 1000))).alias("hh"),
        )
        df_cg_id_agg = df_columns_selected.groupby(
            F.col("advertiser_id"),
            F.col("campaign_group_id"),
            F.col("flight_id"),
            F.col("cgid_mod"),
            F.col("dt").alias("dt"),
            F.col("hh").alias("hh"),
        ).agg(
            F.count(F.col("auction_id")).alias("total_bid_requests"),
            F.sum(F.when(F.col("has_price").cast("boolean"), 1).otherwise(0)).alias("total_bids"),
            F.sum(F.when(~F.col("has_price").cast("boolean"), 1).otherwise(0)).alias(
                "total_204_bids"
            ),
            F.sum(F.col("price")).alias("bid_price_per_hour"),
            F.max(F.col("flight_campaign_group_spend")).alias("max_flight_campaign_group_spend"),
            F.max(F.col("flight_campaign_group_spend_cap")).alias(
                "max_flight_campaign_group_spend_cap"
            ),
            F.max(F.col("flight_campaign_impression")).alias("max_flight_campaign_impression"),
            F.max(F.col("flight_campaign_impression_cap")).alias(
                "max_flight_campaign_impression_cap"
            ),
            F.max(F.col("flight_campaign_group_impression")).alias(
                "max_flight_campaign_group_impression"
            ),
            F.max(F.col("flight_campaign_group_impression_cap")).alias(
                "max_flight_campaign_group_impression_cap"
            ),
            F.max(F.col("daily_campaign_group_spend")).alias("max_daily_campaign_group_spend"),
            F.max(F.col("daily_campaign_group_spend_cap")).alias(
                "max_daily_campaign_group_spend_cap"
            ),
            F.max(F.col("daily_campaign_group_impression")).alias(
                "max_daily_campaign_group_impression"
            ),
            F.max(F.col("daily_campaign_group_impression_cap")).alias(
                "max_daily_campaign_group_impression_cap"
            ),
        )

        df_cam_agg = df_columns_selected.groupBy(
            F.col("advertiser_id"),
            F.col("flight_id"),
            F.col("campaign_group_id"),
            F.col("campaign_id"),
            F.col("cgid_mod").alias("cgid_mod"),
            F.col("dt").alias("dt"),
            F.col("hh").alias("hh"),
        ).agg(
            F.count(F.col("auction_id")).alias("total_bid_requests"),
            F.sum(F.when(F.col("has_price").cast("boolean"), 1).otherwise(0)).alias("total_bids"),
            F.sum(F.when(~F.col("has_price").cast("boolean"), 1).otherwise(0)).alias(
                "total_204_bids"
            ),
            F.sum(F.col("price")).alias("bid_price_per_hour"),
            F.max(F.col("flight_campaign_spend")).alias("max_flight_campaign_spend"),
            F.max(F.col("flight_campaign_spend_cap")).alias("max_flight_campaign_spend_cap"),
            F.max(F.col("daily_campaign_spend")).alias("max_daily_campaign_spend"),
            F.max(F.col("daily_campaign_spend_cap")).alias("max_daily_campaign_spend_cap"),
            F.max(F.col("flight_campaign_impression")).alias("max_flight_campaign_impression"),
            F.max(F.col("flight_campaign_impression_cap")).alias(
                "max_flight_campaign_impression_cap"
            ),
            F.max(F.col("daily_campaign_impression")).alias("max_daily_campaign_impression"),
            F.max(F.col("daily_campaign_impression_cap")).alias(
                "max_daily_campaign_impression_cap"
            ),
        )

        df_terms_flattened = df_columns_selected.select(
            F.col("dt").alias("dt"),
            F.col("hh").alias("hh"),
            F.col("cgid_mod").alias("cgid_mod"),
            F.col("advertiser_id"),
            F.col("flight_id"),
            F.col("campaign_group_id"),
            F.col("campaign_id"),
            F.col("eligible_term_id"),
            F.col("picked_term_id"),
            F.col("auction_id"),
            F.col("has_price").cast("boolean"),
            F.col("threshold_failure_reasons"),
            F.explode(F.col("terms")).alias("exploded_terms"),
        )

        df_term_data = df_terms_flattened.select(
            F.col("dt").alias("dt"),
            F.col("hh").alias("hh"),
            F.col("cgid_mod").alias("cgid_mod"),
            F.col("advertiser_id"),
            F.col("flight_id"),
            F.col("campaign_group_id"),
            F.col("campaign_id"),
            F.col("threshold_failure_reasons").alias("threshold_failure_reasons"),
            F.col("auction_id"),
            F.col("eligible_term_id"),
            F.col("picked_term_id"),
            F.col("has_price").cast("boolean"),
            F.col("exploded_terms").getField("id").alias("term_id"),
            F.col("exploded_terms").getField("spend").alias("term_spend"),
            F.col("exploded_terms").getField("spend_cap").alias("term_spend_cap"),
            F.col("exploded_terms").getField("spend_capped").alias("spend_capped"),
            F.col("exploded_terms").getField("rate_limited").alias("rate_limited"),
            F.col("exploded_terms")
            .getField("throttling_percentage")
            .alias("throttling_percentage"),
            F.col("exploded_terms")
            .getField("bid_volume_percentage")
            .alias("bid_volume_percentage"),
        )

        df_term_agg = df_term_data.groupby(
            F.col("dt").alias("dt"),
            F.col("hh").alias("hh"),
            F.col("cgid_mod").alias("cgid_mod"),
            F.col("advertiser_id"),
            F.col("flight_id"),
            F.col("campaign_group_id"),
            F.col("campaign_id"),
            F.col("eligible_term_id").alias("eligible_term_id"),
            F.col("picked_term_id"),
            F.col("term_id"),
        ).agg(
            F.count(F.col("auction_id")).alias("total_bid_requests"),
            F.sum(F.when(F.col("has_price").cast("boolean"), 1).otherwise(0)).alias("total_bids"),
            F.sum(F.when(~F.col("has_price").cast("boolean"), 1).otherwise(0)).alias(
                "total_204_bids"
            ),
            F.sum(F.when(F.col("spend_capped").cast("boolean"), 1).otherwise(0)).alias(
                "total_spend_capped"
            ),
            F.sum(F.when(F.col("rate_limited").cast("boolean"), 1).otherwise(0)).alias(
                "total_rate_limited"
            ),
            F.max(F.col("term_spend")).cast(DecimalType(10, 2)).alias("max_term_spend"),
            F.max(F.col("term_spend_cap")).alias("max_term_spend_cap"),
            F.max(F.col("throttling_percentage")).alias("throttling_percentage"),
            F.max(F.col("bid_volume_percentage")).alias("bid_volume_percentage"),
        )

        df_failure_reasons_cg_id = df_columns_selected.groupby(
            F.col("dt").alias("dt"),
            F.col("hh").alias("hh"),
            F.col("flight_id"),
            F.col("campaign_group_id"),
            F.col("threshold_failure_reasons").alias("cg_threshold_failure_reasons"),
        ).agg(F.count(F.col("auction_id")).alias("fail_count_cg_id"))

        df_cg_id_data = df_cg_id_agg.join(
            df_failure_reasons_cg_id, on=["dt", "hh", "campaign_group_id", "flight_id"], how="outer"
        )

        df_failure_reasons_c_id = df_columns_selected.groupby(
            F.col("dt").alias("dt"),
            F.col("hh").alias("hh"),
            F.col("campaign_group_id"),
            F.col("campaign_id"),
            F.col("threshold_failure_reasons").alias("campaign_threshold_failure_reasons"),
        ).agg(F.count(F.col("auction_id")).alias("fail_count_campaign_id"))

        df_camp_data = df_cam_agg.join(
            df_failure_reasons_c_id,
            on=["dt", "hh", "campaign_group_id", "campaign_id"],
            how="inner",
        )

        return {
            "campaign_group_log_agg_hr": [df_cg_id_data],
            "campaign_log_agg_hr": [df_camp_data],
            "terms_log_agg_hr": [df_term_agg],
        }

    def get_data_source_dfs(
        self,
    ) -> Dict[str, List[DataFrame]]:
        return self._get_bidder_logs_df()

    def save(self, dfs: Dict[str, List[DataFrame]]):  # type: ignore[no-untyped-def]
        logger.log(self.log_level, "Begin 'save'")
        with ThreadPoolExecutor() as executer:
            for key, data_frames in dfs.items():
                outpath = f"{self.s3_path_out}/{key}"
                for df in data_frames:
                    executer.submit(self.save_parquet(path=outpath, df=df))
            logger.log(self.log_level, "End 'save'")

    def save_parquet(self, path: str, df: DataFrame):  # type: ignore[no-untyped-def]
        df.write.mode("append").partitionBy(*self.partition_columns).parquet(path)

    def populate(self):  # type: ignore[no-untyped-def]
        logger.log(self.log_level, "Begin 'populate'")
        self.save(self.get_data_source_dfs())


def main() -> None:
    parser = argparse.ArgumentParser()

    def parse_date(date_str):  # type: ignore[no-untyped-def]
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    parser.add_argument(
        "-e",
        "--environment",
        choices=["dev", "prod"],
        default="dev",
        help="The environment to run in (either 'dev' or 'prod').",
    )
    parser.add_argument(
        "-dd",
        "--date",
        type=str,
        required=False,
        help="The date to populate the data source id, formatted as 'YYYY-MM-DD'.",
    )
    parser.add_argument(
        "-hh",
        "--hour",
        required=False,
        type=str,
        help="optional hour argument for data",
    )
    parser.add_argument(
        "-mods",
        "--mods",
        required=False,
        type=str,
        help="optional cgid_mod values for IHP",
    )
    # get the command line args
    args = parser.parse_args()
    logger.info(args)
    logger.info(args.date)
    logger.info(args.hour)
    yesterday_date = (datetime.now() - timedelta(days=1)).date()
    yesterday_date_str = yesterday_date.strftime("%Y-%m-%d")
    if not args.mods:
        mods = [999]
    else:
        mods = list(map(int, args.mods.split()))

    BidderLogAggregationHour(
        env=args.environment,
        data_source_date=args.date if args.date else yesterday_date_str,
        data_source_hour=args.hour if args.hour else None,
        cgid_mods=mods,
    ).populate()


if __name__ == "__main__":
    main()
