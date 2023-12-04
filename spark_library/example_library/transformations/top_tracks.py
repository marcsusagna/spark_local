from pyspark.sql import (
    DataFrame,
    SparkSession,
    functions as F,
    types as T,
    Window
)

from example_library.utils.spark_job import SparkJob
from example_library.utils.data_health_checks import (
    null_check,
    uniqueness_check,
    invalid_string,
    timestamp_format_check
)

# Assumptions on input dataframe that will need to be monitored:
# 1) user_id (_c0) and track_start (_c1) are not null. Check behavior: Fail
# (meaningless window function, could aggregate over users for which user id is null)
# 2) track_start (_c1) always abides with TIMESTAMP_FORMAT. Check behavior: Fail (tracks attached to wrong session)
# 3) Columns (_c0) and (_c1) don't have quotes to be scaped because they are not free text, hence (_c3) can't be null
# No need to monitor.
# 4) Combination of user_id and track_start_timestamp should be unique. Check behavior: Warn. Doesn't invalidate logic
# but raises a warning on the quality of the feed
# 5) Artist name (_c3) and track_name (_c5) Don't have ARTIST_TRACK_DELIMITER in them. Check behavior: warn
# A violation would just make the track_name harder to retrieve.

CLASS_NAME="TopTracks"
JOB_NAME="top_tracks"

class TopTracks(SparkJob):
    INPUT_FILE_NAME="userid-timestamp-artid-artname-traid-traname.tsv"
    OUTPUT_FILE_NAME="top_tracks_in_sessions"
    TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
    INPUT_FILE_DELIMITER = "\t"
    ARTIST_TRACK_DELIMITER = "--/"
    SESSION_THRESHOLD_MINUTES = 20
    NUM_TOP_SESSIONS = 50
    NUM_TOP_TRACKS = 10

    def __init__(self, job_name: str, path_to_data_folder: str):
        super().__init__(job_name, path_to_data_folder)

    def _run_data_quality_checks(self, input_df):
        # The spark transformation is based on some data assumptions that need to be checked every time new data needs to be processed
        # Otherwise the computation might be wrong. Depending on the severity of breaking the assumption, the checks should
        # either fail the transform or just warn about it

        null_check(input_df, columns_to_check=["_c0", "_c1"], except_on_fail=True)
        # Check for correct timestamp formatting (assumption 2)
        timestamp_format_check(
            input_df,
            column_to_check="_c1",
            expected_format=self.TIMESTAMP_FORMAT,
            except_on_fail=True
        )

        uniqueness_check(input_df, columns_to_check=["_c0", "_c1"], except_on_fail=False)

        # Check for:
        # Artist name doesn't include the given delimiter between artist name and track name (assumption 4)

        invalid_string(
            input_df,
            columns_to_check=["_c2", "_c5"],
            invalid_string=self.ARTIST_TRACK_DELIMITER,
            # If this check fails, the consequence is a top played track might not be easily recognizable by the name
            except_on_fail=False
        )

    def _clean_raw(self, input_df: DataFrame) -> DataFrame:
        """
        Step 1 from Bronze to Silver: Clean data and enforce schema

        # Why are we creating column full_track_name?
        # Best would be to use track_id to identify song counts in the last step, however, 11% are null (and not due to parsing)
        # Therefore, we create full_track_name for the following reasons:
        # 1) Be able to better identify the track in case it is picked as top 10 (two artists can have the same song name)
        # 2) Remove null values in track_name due to quotes in column artist_name (_c3)
        """

        # Dropping columns that are useless to reduce data shuffling
        input_df = (
            input_df
            .withColumn(
                "full_track_name",
                F.concat_ws(self.ARTIST_TRACK_DELIMITER, F.col("_c3"), F.coalesce(F.col("_c5"), F.lit("")))
            )
            # We are explicit with the format to ensure input comes in a constant format
            .withColumn("track_start_ts", F.to_timestamp(F.col("_c1"), self.TIMESTAMP_FORMAT))
            .selectExpr([
                "_c0 as user_id",
                "track_start_ts",
                "full_track_name"
            ])
        )

        return input_df

    def _enrich_clean(self, all_plays_df: DataFrame) -> DataFrame:
        """
        Step 2 from Bronze to Silver: Enrich dataset

        Compare each track played with the previoulsy played track for each user. Use the minutes between the tracks
        as a way to identify sessions.

        A session is identified by the user_id column and the session_id_ts, which is the starting ts of the session.

        :param all_plays_df: Dataframe with columns: user_id, track_start_ts and full_track_name
        :return: DF with user_id, track_start_ts, full_track_name and session_id_ts
        """

        window_for_lag = (
            Window
            .partitionBy("user_id")
            .orderBy(F.col("track_start_ts").asc())
        )

        window_for_session_id = (
            Window
            .partitionBy("user_id")
            .orderBy(F.col("track_start_ts").asc())
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

        # Create session_id_ts for tracks that initiate a new session
        # Could do it without storing tmp columns, but for readibility / debugging, the intermediate columns are kept
        all_plays_df_with_session = (
            all_plays_df
            .withColumn("lagged_ts", F.lag(F.col("track_start_ts")).over(window_for_lag))
            .withColumn("mins_between_songs",
                        (F.col("track_start_ts").cast(T.LongType()) - F.col("lagged_ts").cast(T.LongType())) / 60)
            .withColumn(
                "session_id_ts",
                F.when(
                    F.col("mins_between_songs") > self.SESSION_THRESHOLD_MINUTES, F.col("track_start_ts")
                ).when(
                    # First ever played track by a user
                    F.col("mins_between_songs").isNull(), F.col("track_start_ts")
                ).otherwise(
                    F.lit(None).cast(T.TimestampType()))
            )
            # Populate session id ts for tracks that don't start a session
            # Tracks that start a session will use the value of session_id_ts, as it is the max until that row
            .withColumn("session_id_ts", F.max("session_id_ts").over(window_for_session_id))
            .selectExpr(
                "user_id",
                "track_start_ts",
                "full_track_name",
                "session_id_ts"
            )
        )

        return all_plays_df_with_session

    def _bronze_to_silver(self, input_df: DataFrame) -> DataFrame:
        cleaned_df=self._clean_raw(input_df)
        enriched_df=self._enrich_clean(cleaned_df)
        return enriched_df

    def _obtain_top_session(self, df_with_session: DataFrame) -> DataFrame:
        """
        Obtain top longest NUM_TOP_SESSIONS by counting number of entries per session

        :param df_with_session: df with all tracks played and the necessary columns user_id and session_id_ts
        :return: Dataframe containing the user_id, session_id_ts for the top longest sessions
        """
        top_sessions = (
            df_with_session
            .groupBy("user_id", "session_id_ts")
            .agg(
                F.count("*").alias("session_length")
            )
            .orderBy(F.col("session_length").desc()).limit(self.NUM_TOP_SESSIONS)
            .drop("session_length")
        )
        return top_sessions


    def _obtain_most_popular_tracks(
            self,
            df_tracks_played: DataFrame,
            df_top_session: DataFrame
        ) -> DataFrame:
        """
        Count track plays within the top longest sessions

        :param df_tracks_played: output df of obtain_session_id
        :param df_top_session: output df of obtain_top_session
        :return: DataFrame with 3 columns (artist_name, track_name and number of times a song was played) for the top songs
        """
        # Broadcast join since df_top_session is most likely below threshold
        tracks_in_top_sessions = (
            df_tracks_played
            .join(
                df_top_session,
                on=["user_id", "session_id_ts"],
                how="inner"
            )
        )

        top_tracks = (
            tracks_in_top_sessions
            .groupBy("full_track_name")
            .agg(
                F.count("*").alias("track_plays")
            )
            .orderBy(F.col("track_plays").desc())
            .limit(self.NUM_TOP_TRACKS)
        )

        top_tracks = (
            top_tracks
            .withColumn("split_name", F.split(F.col("full_track_name"), self.ARTIST_TRACK_DELIMITER))
            .withColumn("artist_name", F.col("split_name").getItem(0))
            .withColumn("track_name", F.col("split_name").getItem(1))
            .selectExpr(
                "artist_name",
                "track_name",
                "track_plays"
            )
        )
        return top_tracks

    def read_inputs(self) -> DataFrame:
        """
        This is step to Bronze layer: Ingestion
        scaping needs to be improved
        :param spark_session:
        :param file_path:
        :return:
        """
        input_df = (
            self.spark
            .read
            .option("quote", "\"")
            .option("escape", "\'")
            .csv(
                f"{self.storage_manager.input_dir}/{self.INPUT_FILE_NAME}", sep=self.INPUT_FILE_DELIMITER, header=False)
        )
        return input_df

    def transformation_logic(self, source_df: DataFrame) -> DataFrame:
        silver_df=self._bronze_to_silver(source_df)
        df_top_sessions=self._obtain_top_session(silver_df)
        df_top_tracks=self._obtain_most_popular_tracks(
            df_tracks_played=silver_df,
            df_top_session=df_top_sessions
        )
        return df_top_tracks
    
    def write_output(self, input_df: DataFrame):
        self.storage_manager.write_to_disk(input_df, self.OUTPUT_FILE_NAME)
    
    def main(self):
        input_df=self.read_inputs()
        #self._run_data_quality_checks(input_df)
        final_df=self.transformation_logic(input_df)
        self.write_output(final_df)