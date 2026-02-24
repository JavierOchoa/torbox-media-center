import json
from pathlib import Path

import pytest

from functions import torboxFunctions as torbox


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "metadata_true_blood_cases.json"
FIXTURE_DATA = json.loads(FIXTURE_PATH.read_text())


class MockResponse:
    def __init__(self, data, status_code=200, text="OK"):
        self._data = data
        self.status_code = status_code
        self.text = text
        self.headers = {}

    def json(self):
        return {"data": self._data}


def install_search_mock(monkeypatch, candidates):
    call_counter = {"count": 0}

    def fake_request_wrapper(client, method, url, **kwargs):
        call_counter["count"] += 1
        return MockResponse(candidates)

    monkeypatch.setattr(torbox, "requestWrapper", fake_request_wrapper)
    return call_counter


def process_pack(item, files):
    results = []
    for file_data in files:
        result = torbox.process_file(item, dict(file_data), torbox.DownloadType.torrent)
        results.append(result)
    return results


def assert_pack_result_matches_expected(results, expected):
    assert all(result is not None for result in results)
    assert [result["metadata_episode"] for result in results] == expected["episodes"]
    assert [result["metadata_filename"] for result in results] == expected["filenames"]
    for result in results:
        assert result["metadata_title"] == "True Blood"
        assert result["metadata_rootfoldername"] == expected["root_folder"]
        assert result["metadata_foldername"] == expected["season_folder"]


def test_true_blood_s07_hash_pack_maps_to_expected_season_folder(monkeypatch):
    pack = FIXTURE_DATA["packs"]["true_blood_s07_hash"]
    candidates = FIXTURE_DATA["candidates"]["true_blood_webisodes_first"]
    call_counter = install_search_mock(monkeypatch, candidates)

    item = dict(pack["item"])
    results = process_pack(item, pack["files"])

    assert_pack_result_matches_expected(results, pack["expected"])
    assert call_counter["count"] == 1


def test_true_blood_s01_pack_stays_correct_with_webisodes_candidate(monkeypatch):
    pack = FIXTURE_DATA["packs"]["true_blood_s01_named"]
    candidates = FIXTURE_DATA["candidates"]["true_blood_webisodes_first"]
    call_counter = install_search_mock(monkeypatch, candidates)

    item = dict(pack["item"])
    results = process_pack(item, pack["files"])

    assert_pack_result_matches_expected(results, pack["expected"])
    assert call_counter["count"] == 1


def test_series_identity_cache_reuses_lookup_across_different_items(monkeypatch):
    candidates = FIXTURE_DATA["candidates"]["true_blood_webisodes_first"]
    s07_pack = FIXTURE_DATA["packs"]["true_blood_s07_hash"]
    call_counter = install_search_mock(monkeypatch, candidates)

    first_item = dict(s07_pack["item"])
    second_item = {
        "id": 9002,
        "name": "another-true-blood-pack-hash",
        "hash": "another-true-blood-pack-hash",
        "cached": True,
    }

    first_result = torbox.process_file(first_item, dict(s07_pack["files"][0]), torbox.DownloadType.torrent)
    second_result = torbox.process_file(second_item, dict(s07_pack["files"][1]), torbox.DownloadType.torrent)

    assert first_result is not None
    assert second_result is not None
    assert second_result["metadata_rootfoldername"] == "True Blood (2008)"
    assert second_result["metadata_foldername"] == "Season 7"
    assert second_result["metadata_episode"] == 2
    assert call_counter["count"] == 1


def test_series_candidate_is_preferred_over_movie_for_episode_like_files(monkeypatch):
    candidates = FIXTURE_DATA["candidates"]["true_blood_series_and_movie"]
    call_counter = install_search_mock(monkeypatch, candidates)

    metadata, success, _ = torbox.searchMetadata(
        query="True Blood",
        title_data={"title": "True Blood", "year": 2008},
        file_name="True.Blood.S07E01.mkv",
        full_title="True Blood True.Blood.S07E01.mkv",
        hash="series-vs-movie-hash",
        item_name="True Blood Season 7",
        cache_key="series-vs-movie-cache-key",
        parsed_season=7,
        parsed_episode=1,
        is_special_request=False,
        item_identity_cache_key=None,
        series_identity_cache_keys=[],
    )

    assert success is True
    assert metadata["metadata_mediatype"] == "series"
    assert metadata["metadata_foldername"] == "Season 7"
    assert metadata["metadata_filename"] == "True Blood S07E01.mkv"
    assert call_counter["count"] == 1


def test_explicit_special_episode_maps_to_specials_folder(monkeypatch):
    candidates = FIXTURE_DATA["candidates"]["true_blood_webisodes_first"]
    install_search_mock(monkeypatch, candidates)

    metadata, success, _ = torbox.searchMetadata(
        query="True Blood",
        title_data={"title": "True Blood"},
        file_name="True.Blood.S00E01.Special.Featurette.mkv",
        full_title="True Blood True.Blood.S00E01.Special.Featurette.mkv",
        hash="tb-specials-hash",
        item_name="True Blood Specials",
        cache_key="specials-cache-key",
        parsed_season=0,
        parsed_episode=1,
        is_special_request=True,
        item_identity_cache_key=None,
        series_identity_cache_keys=[],
    )

    assert success is True
    assert metadata["metadata_foldername"] == "Specials"
    assert metadata["metadata_season"] == 0
    assert metadata["metadata_episode"] == 1


def test_low_confidence_series_lookup_falls_back_safely(monkeypatch):
    def fake_request_wrapper(client, method, url, **kwargs):
        return MockResponse(
            [
                {
                    "title": "Completely Different Documentary",
                    "type": "movie",
                    "releaseYears": "1999",
                    "link": "https://example.com/other",
                    "image": None,
                    "backdrop": None,
                }
            ]
        )

    monkeypatch.setattr(torbox, "requestWrapper", fake_request_wrapper)

    metadata, success, detail = torbox.searchMetadata(
        query="True Blood",
        title_data={"title": "True Blood", "year": 2008},
        file_name="True.Blood.S07E01.mkv",
        full_title="True Blood True.Blood.S07E01.mkv",
        hash="tb-low-confidence",
        item_name="True Blood Season 7",
        cache_key=None,
        parsed_season=7,
        parsed_episode=1,
        is_special_request=False,
        item_identity_cache_key=None,
        series_identity_cache_keys=[],
    )

    assert success is False
    assert metadata["metadata_mediatype"] == "movie"
    assert metadata["metadata_rootfoldername"] == "True Blood Season 7"
    assert "No confident metadata found" in detail or "Series metadata could not be confidently matched" in detail


@pytest.mark.parametrize(
    "file_name,file_path,expected",
    [
        ("True.Blood.S07E01.mkv", "True Blood/Season 7/True.Blood.S07E01.mkv", (7, 1, False)),
        ("True.Blood.7x01.mkv", "True Blood/Season 7/True.Blood.7x01.mkv", (7, 1, False)),
        ("True Blood Season 7 Episode 1.mkv", "True Blood/Season 7/episode.mkv", (7, 1, False)),
        ("episode-01.mkv", "True Blood/Season 7/episode-01.mkv", (7, None, False)),
    ],
)
def test_parse_season_episode_variants(file_name, file_path, expected):
    parsed = torbox.getParsedSeasonEpisode({}, file_name, file_path)
    assert parsed == expected


def test_parse_season_episode_ignores_common_noise_tokens():
    parsed = torbox.getParsedSeasonEpisode({}, "True.Blood.1080p.BluRay.x264.mkv", "True Blood/collection/file.mkv")
    assert parsed == (None, None, False)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("True Blood Specials", True),
        ("True Blood: Webisodes", True),
        ("specialized-edition", False),
        ("True Blood Season 7", False),
    ],
)
def test_special_keyword_detection(value, expected):
    assert torbox.containsSpecialKeyword(value) is expected


def test_failure_cache_avoids_repeat_search_until_ttl_expiry(monkeypatch):
    current_time = {"value": 1_700_000_000}

    def fake_time():
        return current_time["value"]

    call_counter = {"count": 0}

    def fake_request_wrapper(client, method, url, **kwargs):
        call_counter["count"] += 1
        return MockResponse([])

    monkeypatch.setattr(torbox.time, "time", fake_time)
    monkeypatch.setattr(torbox, "requestWrapper", fake_request_wrapper)

    kwargs = {
        "query": "True Blood",
        "title_data": {"title": "True Blood", "year": 2008},
        "file_name": "True.Blood.S07E01.mkv",
        "full_title": "True Blood True.Blood.S07E01.mkv",
        "hash": "failure-cache-hash",
        "item_name": "True Blood Season 7",
        "cache_key": "failure-cache-key",
        "parsed_season": 7,
        "parsed_episode": 1,
        "is_special_request": False,
        "item_identity_cache_key": None,
        "series_identity_cache_keys": [],
    }

    _, success_first, _ = torbox.searchMetadata(**kwargs)
    _, success_second, _ = torbox.searchMetadata(**kwargs)

    assert success_first is False
    assert success_second is False
    assert call_counter["count"] == 1

    current_time["value"] += torbox.METADATA_FAILURE_CACHE_TTL_SECONDS + 1
    torbox.searchMetadata(**kwargs)
    assert call_counter["count"] == 2
