import pytest

pytest.mark.usefixtures("spark_session", "top_tracks", "input_df", "top_sessions_expected_df", "top_tracks_expected_df")


class TestTopTracks:
    """
    Test Suite for the logic to compute the top tracks out of the longest sessions
    """

    def test_top_sessions(self, top_tracks, input_df, top_sessions_expected_df):
        plays_with_session = top_tracks._enrich_clean(input_df)
        top_sessions = top_tracks._obtain_top_session(plays_with_session)
        assert top_sessions.collect() == top_sessions_expected_df.collect()

    def test_top_tracks(self, top_tracks, input_df, top_tracks_expected_df):
        plays_with_session = top_tracks._enrich_clean(input_df)
        top_sessions = top_tracks._obtain_top_session(plays_with_session)
        top_tracks = top_tracks._obtain_most_popular_tracks(plays_with_session, top_sessions)
        # Careful: The count is forced to be a longType. If this was double, due to float precision and
        # randomness of execution order in a distributed environment (which executor finishes first?)
        # Then this test would fail if we don't add some acceptance bounds
        assert top_tracks.collect() == top_tracks_expected_df.collect()