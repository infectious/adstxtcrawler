import logging

import pytest

from adstxt import fetch


USER_AGENT = 'adstxt_integration_test'


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetch_problem_urls(caplog):
    """Test problem URLs that have tripped us up before."""
    caplog.set_level(logging.DEBUG)

    # Has a basic adstxt all direct through Google.
    michelin = await fetch.fetch('viamichelin.co.uk', USER_AGENT)
    assert michelin.adstxt_present is True
    # Michelin just sell through Adx.
    assert len(michelin.response) >= 1

    capital = await fetch.fetch('capital.it', USER_AGENT)
    assert capital.adstxt_present is True
    assert len(capital.response) > 1

    amazon_redirect = await fetch.fetch('nice-video.de', USER_AGENT)
    assert amazon_redirect.adstxt_present is True

    framboise = await fetch.fetch('framboise314.fr', USER_AGENT)
    assert framboise.adstxt_present is True
    assert len(framboise.response) > 1

    lucifer = await fetch.fetch('lucifer.wikia.com', USER_AGENT)
    assert lucifer.adstxt_present is True


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("test_input,expected", [
    ("ebay.co.uk", True),
    ("ebay.de", True),
    ("ebay.it", True),
    ("ebay.fr", True),
    ("m.ebay.co.uk", True),
    ("ebay.com", True),
    ("m.ebay.de", True),
    ("ebay.es", True),
    ("m.ebay.it", True),
    ("m.ebay.fr", True),
])
async def test_ebay_subdomains(test_input, expected, caplog):
    caplog.set_level(logging.DEBUG)

    response = await fetch.fetch(test_input, USER_AGENT)
    assert response.adstxt_present == expected
