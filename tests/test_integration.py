import os

import pytest

from githarbor import create_repository


BITBUCKET_TOKEN = os.getenv("BITBUCKET_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_URL = "https://github.com/phil65/mknodes"
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("GITHUB_TOKEN"), reason="GITHUB_TOKEN not set")
def test_github_integration():
    repo = create_repository(GITHUB_URL, token=GITHUB_TOKEN)
    assert repo.name == "mknodes"  # Updated to match actual repo name


@pytest.mark.integration
def test_gitlab_integration():
    repo = create_repository("https://gitlab.com/phil65/test", token=GITLAB_TOKEN)
    assert repo.name == "test"


# @pytest.mark.integration
# def test_bitbucket_integration():
#     repo = create_repository(
#         "https://bitbucket.org/phil__65/testrepo/", token=BITBUCKET_TOKEN
#     )
#     assert repo.name == "testrepo"
