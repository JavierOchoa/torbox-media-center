from functions import torboxFunctions as torbox


class MockResponse:
    def __init__(self, data, status_code=200, text="OK"):
        self._data = data
        self.status_code = status_code
        self.text = text
        self.headers = {}

    def json(self):
        return {"data": self._data}


def build_true_blood_candidates():
    return [
        {
            "title": "True Blood Specials",
            "type": "series",
            "releaseYears": "2008",
            "link": "https://example.com/specials",
            "image": None,
            "backdrop": None,
        },
        {
            "title": "True Blood",
            "type": "series",
            "releaseYears": "2008",
            "link": "https://example.com/true-blood",
            "image": None,
            "backdrop": None,
        },
    ]


def test_series_episode_prefers_main_show_over_specials(monkeypatch):
    call_counter = {"count": 0}

    def fake_request_wrapper(client, method, url, **kwargs):
        call_counter["count"] += 1
        return MockResponse(build_true_blood_candidates())

    monkeypatch.setattr(torbox, "requestWrapper", fake_request_wrapper)

    item = {
        "id": 1001,
        "name": "True Blood Season 7",
        "hash": "tb-season-pack",
        "cached": True,
    }
    file = {
        "id": 1,
        "short_name": "True.Blood.S07E01.1080p.WEB-DL.x264.mkv",
        "name": "True Blood Season 7/True.Blood.S07E01.1080p.WEB-DL.x264.mkv",
        "mimetype": "video/x-matroska",
        "size": 123,
    }

    result = torbox.process_file(item, file, torbox.DownloadType.torrent)

    assert result is not None
    assert result["metadata_title"] == "True Blood"
    assert result["metadata_foldername"] == "Season 7"
    assert result["metadata_season"] == 7
    assert result["metadata_episode"] == 1
    assert result["metadata_rootfoldername"] == "True Blood (2008)"
    assert call_counter["count"] == 1


def test_explicit_special_episode_maps_to_specials_folder(monkeypatch):
    def fake_request_wrapper(client, method, url, **kwargs):
        return MockResponse(build_true_blood_candidates())

    monkeypatch.setattr(torbox, "requestWrapper", fake_request_wrapper)

    item = {
        "id": 1002,
        "name": "True Blood Specials",
        "hash": "tb-special-pack",
        "cached": True,
    }
    file = {
        "id": 2,
        "short_name": "True.Blood.S00E01.Special.Featurette.mkv",
        "name": "True Blood Specials/True.Blood.S00E01.Special.Featurette.mkv",
        "mimetype": "video/x-matroska",
        "size": 456,
    }

    result = torbox.process_file(item, file, torbox.DownloadType.torrent)

    assert result is not None
    assert result["metadata_foldername"] == "Specials"
    assert result["metadata_season"] == 0
    assert result["metadata_episode"] == 1


def test_item_identity_cache_reuses_first_series_match(monkeypatch):
    call_counter = {"count": 0}

    def fake_request_wrapper(client, method, url, **kwargs):
        call_counter["count"] += 1
        return MockResponse(build_true_blood_candidates())

    monkeypatch.setattr(torbox, "requestWrapper", fake_request_wrapper)

    item = {
        "id": 2001,
        "name": "True Blood Complete Season 7",
        "hash": "tb-complete-pack",
        "cached": True,
    }

    first_file = {
        "id": 11,
        "short_name": "True.Blood.S07E01.mkv",
        "name": "True Blood Complete Season 7/True.Blood.S07E01.mkv",
        "mimetype": "video/x-matroska",
        "size": 100,
    }
    second_file = {
        "id": 12,
        "short_name": "True.Blood.S07E02.mkv",
        "name": "True Blood Complete Season 7/True.Blood.S07E02.mkv",
        "mimetype": "video/x-matroska",
        "size": 100,
    }

    first = torbox.process_file(item, first_file, torbox.DownloadType.torrent)
    second = torbox.process_file(item, second_file, torbox.DownloadType.torrent)

    assert first is not None
    assert second is not None
    assert call_counter["count"] == 1
    assert first["metadata_rootfoldername"] == second["metadata_rootfoldername"]
    assert second["metadata_foldername"] == "Season 7"
    assert second["metadata_episode"] == 2


def test_low_confidence_series_lookup_falls_back(monkeypatch):
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


def test_parse_season_from_folder_path_fallback():
    season, episode, is_special = torbox.getParsedSeasonEpisode(
        title_data={},
        file_name="episode-01.mkv",
        file_path="True Blood/Season 7/episode-01.mkv",
    )

    assert season == 7
    assert episode is None
    assert is_special is False
