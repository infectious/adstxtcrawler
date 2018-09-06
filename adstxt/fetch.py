from asyncio import TimeoutError, sleep, BoundedSemaphore
import datetime
import logging
from typing import Optional, NamedTuple, Tuple

import async_timeout  # type: ignore
from aiohttp import ClientSession, client_exceptions as exceptions
import tldextract


log = logging.getLogger(__name__)

MAX_CONCURRENT_REQUESTS = 100
TIMEOUT = 5
_SEMAPHORE = BoundedSemaphore(value=MAX_CONCURRENT_REQUESTS)


class FetchResponse(NamedTuple):
    domain: str
    scraped_at: datetime.datetime
    adstxt_present: Optional[bool]
    response: Tuple[str, ...]


async def fetch(domain: str, user_agent: str) -> FetchResponse:
    """Fetch a domain over http, check for validity and return.

    Args
        Domain (str): string domain to fetch.

    Returns
        FetchResponse (NamedTuple): Reponse tuple with all data.

    """
    unprocessable = FetchResponse(domain=domain,
                                  scraped_at=datetime.datetime.utcnow(),
                                  adstxt_present=False,
                                  response=())

    # TODO: Discuss making this HTTPS and fallback on HTTP.
    url = 'http://' + domain + '/ads.txt'

    # Don't put anything like a 'scraper' name in here, as people do silly
    # filtering on web pages for bots.  Yes, they even filter pages that
    # are supposed to be accessed by bots. If anything change it to chromes UA.
    headers = {'User-Agent': user_agent}

    async with _SEMAPHORE:
        async with ClientSession() as session:
            for attempt in range(5):
                try:
                    async with async_timeout.timeout(TIMEOUT):
                        try:
                            async with session.get(
                                    url, headers=headers) as response:
                                if response.status == 200:
                                    text = await response.text()
                                    break
                        # Frequently we're seeing redirects that pass through
                        # invalid certificates on CDN/static servers.  The vast
                        # majority of these exceptions are due to people having
                        # a wildcard cert without the root.  Passing through
                        # www.  subdomain normally resolves these issues.
                        except exceptions.ClientConnectorCertificateError:
                            url = 'http://www.' + domain + '/ads.txt'
                        # This catches a whole bunch of low level things in
                        # one. Sockets not being open as well as NXDOMAIN
                        # responses.
                        except exceptions.ClientConnectorError:
                            log.debug('Domain not accepting connections.')
                            return unprocessable
                        # Remotes disconnect for a whole bunch of reasons.
                        # Some instances this is retryable.
                        except exceptions.ServerDisconnectedError:
                            log.debug('Remote disconnected on us, retrying...')
                        # Catch the base exception and log the specific reason.
                        # Mostly here to see if we need to do anything
                        # different.
                        except exceptions.ClientError as excpt:
                            log.warning(
                                'Caught general exception %r on domain %r.',
                                excpt, domain)
                        # TODO: We can do better than just returning
                        # unprocessable here.  Find the line with the bad
                        # unicode and work round it?
                        except UnicodeDecodeError:
                            log.debug(
                                'Invalid unicode found on %r, skipping.',
                                domain)
                            return unprocessable
                except TimeoutError:
                    log.debug('Fetch timeout, backing off and retrying.')
                    await sleep(attempt ** 2)
            # No break was caused, return unprocessable.
            else:
                log.debug(
                    'Unable to fetch for %s due to max attempts reached.',
                    domain)
                return unprocessable

            # Check to see if we're still on the right domain.
            if len(response.history) != 0:
                log.debug(
                    '%r domain used a redirect, validating this.', domain)
                root_domain = tldextract.extract(domain).domain
                log.debug('root domain found to be %r.', root_domain)
                # Get the destination domain of the final location.
                destination_location = tldextract.extract(
                    response.history[-1].url.host).domain
                #  Multiple redirects are valid as long as each redirect
                # location remains within the original root domain.  Check to
                # see if where we are redirected to is the same domain as the
                # fetched domain.
                if root_domain != destination_location:
                    # Domain is found to not be the same as the one we tried
                    # (or www redirects which we ignore).
                    log.info('%r uses an off domain redirect %r',
                             domain, destination_location)
                    # We only allow 1 hop when going off domain.  Get the last
                    # but one redirect and check to see if it's on the same
                    # domain.
                    redirection_domain = tldextract.extract(
                        response.history[-2].url.host).domain
                    if root_domain != redirection_domain:
                        log.info(
                            '%r uses an invalid off domain redirect to %r',
                            domain, destination_location)
                        return unprocessable
                    else:
                        log.info('%r off domain redirect valid.', domain)
                else:
                    log.debug('%r uses on domain redirect.', domain)

        # If the content type of the response isn't text, return unprocessable.
        if 'text/plain' not in response.headers.get('Content-Type', ''):
            return unprocessable

    # If we're getting a HTML page back then somethings fuckity.
    # We do this because sometimes 404 pages and the like don't have
    # the correct content-type header set.
    html_elements = ['<!doctype html', '<img', '<div class']
    for element in html_elements:
        if element in text:
            log.debug(
                'HTML elements found in %r domains adstxt.', domain)
            return unprocessable

    # Normalise to a tuple, split on new line and strip returns.
    response = tuple(x.strip('\r') for x
                     in text.split('\n')
                     if x.strip('\r'))

    return FetchResponse(domain=domain,
                         scraped_at=datetime.datetime.utcnow(),
                         adstxt_present=True,
                         response=response)
