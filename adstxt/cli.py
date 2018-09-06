import logging
import os
import sys

import click
from raven import Client  # type: ignore
from raven.handlers.logging import SentryHandler  # type: ignore
from raven.conf import setup_logging  # type: ignore

from adstxt.main import AdsTxtCrawler
from adstxt.exceptions import ConfigurationError


log = logging.getLogger(__name__)


@click.command()
@click.option('--db_uri', envvar='ADSTXT_DB_URI', required=True)
@click.option('--es_uri', envvar='ADSTXT_ES_URI')
@click.option('--es_query', envvar='ADSTXT_ES_QUERY')
@click.option('--es_index', envvar='ADSTXT_ES_INDEX')
@click.option('--file_path', envvar='ADSTXT_FILE_PATH')
@click.option('--crawler_tag', envvar='ADSTXT_CRAWLER_TAG', required=True)
@click.option('--log_level', envvar='ADSTXT_LOG_LEVEL', default='INFO')
@click.option('--log_formatter', envvar='ADSTXT_LOG_FORMATTER', default=None)
@click.option('--es', is_flag=True)
@click.option('--file', is_flag=True)
def cli(db_uri,
        es_uri,
        es_query,
        es_index,
        file_path,
        crawler_tag,
        log_level,
        log_formatter,
        es,
        file):  # pragma: no cover
    # Setup a default formatter incase one isn't provided.
    formatter = ('%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                 if not log_formatter else log_formatter)
    # Log to stdout with defaults.
    logging.basicConfig(
        stream=sys.stderr,
        level=log_level.upper(),
        format=formatter)

    log.info('Launching CLI and validating configuration.')

    # Check that the config we've been provided works.
    if not es and not file:
        raise ConfigurationError('Invalid configuration, no input given.')
    if file and not file_path:
        raise ConfigurationError(
            'Invalid configuration, file used but no path chosen.')
    if es and not all([es_query, es_index, es_uri]):
        raise ConfigurationError(
            'Invalid configuration, es used but some configuration is '
            'missing. query=%r, index=%r, uri=%r', es_query, es_index, es_uri)

    crawler = AdsTxtCrawler(es,
                            file,
                            db_uri,
                            es_uri=es_uri,
                            es_query=es_query,
                            es_index=es_index,
                            file_uri=file_path,
                            crawler_id=crawler_tag)

    version_hash = os.environ.get('GIT_HASH')
    sentry = Client(release=version_hash)
    sentry_handler = SentryHandler(sentry, level=logging.WARNING)
    setup_logging(sentry_handler)

    try:
        crawler.run()
    except Exception as e:
        sentry.captureException()
        raise e
