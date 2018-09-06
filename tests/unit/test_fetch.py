from asyncio import TimeoutError
import logging
from unittest.mock import call

import pytest
import asynctest
from aiohttp import client_exceptions

import adstxt.fetch as fetch


DUMMY_FETCH_DATA_CR = "foo\n\rbar\n\rbaz\n\r"
DUMMY_FETCH_DATA_NL = "foo\nbar\nbaz\n"

EXPECTED_RESULTS = ("foo", "bar", "baz")

USER_AGENT = 'testings'


class Headers():

    def __init__(self, headers):
        self._headers = headers

    def __call__(self):
        return self._headers

    def get(self, key, if_none):
        return self._headers.get(key, if_none)


class MockSession():
    """This clones the bits of an aiohttp.Session so we can mock less."""

    def __init__(self, text, response, redirect, headers):
        self._text = text
        self._response = response
        self._headers = headers
        self.headers = Headers(self._headers)
        if redirect:
            self._history = redirect
        else:
            self._history = []

    async def __aexit__(self, *args):
        pass

    async def __aenter__(self):
        return self

    @property
    def status(self):
        return self._response

    async def text(self):
        return self._text

    @property
    def history(self):
        return self._history


@pytest.mark.asyncio
async def test_fetch_cr(mocker):
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    mock_get.return_value = MockSession(
        DUMMY_FETCH_DATA_CR, 200, False, {'Content-Type': 'text/plain'})

    test_fetch = await fetch.fetch('localhost', USER_AGENT)

    assert test_fetch.response == EXPECTED_RESULTS
    assert test_fetch.domain == 'localhost'


@pytest.mark.asyncio
async def test_fetch_nl(mocker):
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    mock_get.return_value = MockSession(
        DUMMY_FETCH_DATA_NL, 200, False, {'Content-Type': 'text/plain'})

    test_fetch = await fetch.fetch('localhost', USER_AGENT)

    assert test_fetch.response == EXPECTED_RESULTS
    assert test_fetch.domain == 'localhost'


@pytest.mark.asyncio
async def test_fetch_exceptions(mocker):
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    # Mock out sleep so tests don't take ages.
    mock_sleep = mocker.patch.object(fetch, 'sleep')
    mock_sleep_coroutine_mock = asynctest.CoroutineMock()
    mock_sleep.side_effect = mock_sleep_coroutine_mock
    mock_get.side_effect = [TimeoutError, MockSession(
        DUMMY_FETCH_DATA_NL, 200, False, {'Content-Type': 'text/plain'})]

    test_fetch = await fetch.fetch('localhost', USER_AGENT)

    assert test_fetch.response == EXPECTED_RESULTS
    assert test_fetch.domain == 'localhost'

    # Check we've backed off.
    assert mock_sleep_coroutine_mock.call_count == 1


@pytest.mark.asyncio
async def test_fetch_client_cert_invalid_retry_www(mocker):
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')

    mock_get.side_effect = [
        client_exceptions.ClientConnectorCertificateError('a', 'b'),
        MockSession(
            DUMMY_FETCH_DATA_NL, 200, False, {'Content-Type': 'text/plain'})]

    test_fetch = await fetch.fetch('localhost', USER_AGENT)

    assert test_fetch.response == EXPECTED_RESULTS
    assert test_fetch.domain == 'localhost'

    # Assert that we go away from localhost and check www.localhost
    expected_calls = [call('http://localhost/ads.txt',
                           headers={'User-Agent': 'testings'}),
                      call('http://www.localhost/ads.txt',
                           headers={'User-Agent': 'testings'})]

    assert mock_get.mock_calls == expected_calls


@pytest.mark.asyncio
async def test_fetch_unicode_decode_error(mocker):
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    mock_get.side_effect = UnicodeDecodeError(
        'blah', b'\x00\x00', 1, 2, 'unicode is hard mmmkay')

    test_fetch = await fetch.fetch('localhost', USER_AGENT)

    assert test_fetch.response == ()
    assert test_fetch.domain == 'localhost'
    assert test_fetch.adstxt_present is False


@pytest.mark.asyncio
async def test_fetch_bad_page(mocker):
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    mock_get.return_value = MockSession(
        '<!doctype html></br>', 200, False, {'Content-Type': 'text/plain'})

    test_fetch = await fetch.fetch('localhost', USER_AGENT)
    assert test_fetch.response is ()
    assert test_fetch.domain == 'localhost'
    assert test_fetch.adstxt_present is False


@pytest.mark.asyncio
async def test_fetch_html_content(mocker):
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    mock_get.return_value = MockSession(
        '<!doctype html></br>', 200, False, {'Content-Type': 'text/html'})

    test_fetch = await fetch.fetch('localhost', USER_AGENT)
    assert test_fetch.response is ()
    assert test_fetch.domain == 'localhost'
    assert test_fetch.adstxt_present is False


@pytest.mark.asyncio
async def test_fetch_404(mocker):
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    mock_get.return_value = MockSession(
        '<!doctype html><blink>', 404, False, {'Content-Type': 'text/html'})

    test_fetch = await fetch.fetch('localhost', USER_AGENT)
    assert test_fetch.response is ()
    assert test_fetch.domain == 'localhost'
    assert test_fetch.adstxt_present is False


class History():

    def __init__(self, url):
        self._url = url

    @property
    def url(self):
        return self._url


class Urls():

    def __init__(self, host):
        self._host = host

    @property
    def host(self):
        return self._host


@pytest.mark.asyncio
async def test_fetch_redirects_www_redirect(mocker):
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    redirect_list = [History(Urls('ebay.co.uk')),
                     History(Urls('www.ebay.co.uk'))]
    mock_get.return_value = MockSession(
        DUMMY_FETCH_DATA_CR, 200, redirect_list,
        {'Content-Type': 'text/plain'})

    test_fetch = await fetch.fetch('ebay.co.uk', USER_AGENT)

    assert test_fetch.response == EXPECTED_RESULTS
    assert test_fetch.domain == 'ebay.co.uk'
    assert test_fetch.adstxt_present is True


@pytest.mark.asyncio
async def test_fetch_redirects_subdomain_redirect(mocker, caplog):
    caplog.set_level(logging.DEBUG)
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    # A tripple subdomain redirect checks that we're always valid when staying
    # on the same root domain.
    redirect_list = [History(Urls('bar.foo.com')),
                     History(Urls('baz.foo.com')),
                     History(Urls('qux.foo.com'))]
    mock_get.return_value = MockSession(
        DUMMY_FETCH_DATA_CR, 200, redirect_list,
        {'Content-Type': 'text/plain'})

    test_fetch = await fetch.fetch('bar.foo.com', USER_AGENT)

    assert test_fetch.response == EXPECTED_RESULTS
    assert test_fetch.domain == 'bar.foo.com'
    assert test_fetch.adstxt_present is True


@pytest.mark.asyncio
async def test_fetch_redirects_offdomain_redirect(mocker, caplog):
    caplog.set_level(logging.DEBUG)
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    # A single off domain redirect is valid.
    redirect_list = [History(Urls('foo.com')),
                     History(Urls('foo.bar.com'))]
    mock_get.return_value = MockSession(
        DUMMY_FETCH_DATA_CR, 200, redirect_list,
        {'Content-Type': 'text/plain'})

    test_fetch = await fetch.fetch('foo.com', USER_AGENT)

    assert test_fetch.response == EXPECTED_RESULTS
    assert test_fetch.domain == 'foo.com'
    assert test_fetch.adstxt_present is True


@pytest.mark.asyncio
async def test_fetch_redirects_bad_redirection_two_hops_off_site(mocker):
    mock_get = mocker.patch.object(fetch.ClientSession, 'get')
    redirect_list = [History(Urls('bad-redirect-domain.co.uk')),
                     History(Urls('some-other-domain.co.uk')),
                     History(Urls('some-different-domain.co.uk'))]
    mock_get.return_value = MockSession(
        DUMMY_FETCH_DATA_CR, 200, redirect_list,
        {'Content-Type': 'text/plain'})

    test_fetch = await fetch.fetch('bad-redirect-domain.co.uk', USER_AGENT)

    assert test_fetch.response is ()
    assert test_fetch.domain == 'bad-redirect-domain.co.uk'
    assert test_fetch.adstxt_present is False
