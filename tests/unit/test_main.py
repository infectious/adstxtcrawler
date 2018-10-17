"""Test core crawler functionality."""
import datetime
import logging
import os

import pytest

from adstxt.fetch import FetchResponse
import adstxt.main as main
import adstxt.models as models


log = logging.getLogger(__name__)


BROKEN_FETCH_SENTRY_1023 = FetchResponse(
    'weather.com',
    datetime.datetime(2018, 3, 26, 10, 55, 59, 661410),
    True,
    (
        '#us',
        'amazon-adsystem.com, 1004, DIRECT',
        'adtech.com, 9538, DIRECT',
        'aolcloud.net, 9538, DIRECT',
        'rubiconproject.com, 10738, DIRECT, 0bfd66d529a55807',
        'criteo.com, 137482, DIRECT',
        'appnexus.com, 2678, DIRECT, f5ab79cb980f11d1',
        'coxmt.com, 2000068019502, DIRECT',
        'indexexchange.com, 184272, DIRECT',
        'indexexchange.com, 182970, DIRECT',
        'Openx.com, 537108601, DIRECT',
        'Openx.com, 537150861, DIRECT',
        'facebook.com, 115862191798713, DIRECT',
        'â\x80\x8bquantum-advertising.com, 888, RESELLER ',  # Here is bad.
        'facebook.com, 124523452, DIRECT'
    )
)


@pytest.fixture(scope='function')
def adstxtcrawler(request):
    if os.environ.get('CI'):
        # Setup crawler using Drone database.
        # This is from http://docs.drone.io/mysql-example/
        db_uri = ('mysql+pymysql://root:root@'
                  'database:3306/adstxt?charset=utf8mb4')
    else:
        db_uri = "sqlite:///:memory:"
    es_uri = 'localhost'
    crawler = main.AdsTxtCrawler(True,
                                 False,
                                 db_uri,
                                 es_uri=es_uri,
                                 es_query='{"query": true}',
                                 es_index='adstxt_testings',
                                 crawler_id='unit_test_ua')
    crawler._bootstrap_db()
    crawler._testing = True

    def clean_database():
        logging.info("Removing fixture database.")
        # Ensure all connections are closed first.
        crawler._session.close_all()
        # Drop all databases.
        models.Base.metadata.drop_all(crawler.engine)

    request.addfinalizer(clean_database)

    return crawler


def test_filter_for_domain(adstxtcrawler, mocker):
    pass


def test_check_viability_false(adstxtcrawler, mocker):
    fake_last_crawled = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=5))
    # Yo dawg, I heard you like mocks.
    mock_last_updated_at = mocker.patch.object(
        adstxtcrawler, '_last_updated_at',)
    mocker.patch.object(adstxtcrawler, '_session')
    mock_last_updated_at.return_value = fake_last_crawled

    result = adstxtcrawler._check_viability('independent.co.uk')
    assert result is False, "Crawled too recently."


def test_check_viability_true(adstxtcrawler, mocker):
    fake_last_crawled = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=600))
    # Yo dawg, I heard you like mocks.
    mock_last_updated_at = mocker.patch.object(
        adstxtcrawler, '_last_updated_at',)
    mocker.patch.object(adstxtcrawler, '_session')
    mock_last_updated_at.return_value = fake_last_crawled

    result = adstxtcrawler._check_viability('independent.co.uk')
    assert result is True, "Ready to crawl."


def test_check_viability_bad_domain(adstxtcrawler, mocker):
    mock_validators = mocker.patch.object(
        main.validators, 'domain',)
    mock_validators.return_value = False

    bad_domain = ('fooooooooooooooooooooooooooooooooooooooo'
                  'oooooooooooooooooooooooooooooooooooooooo'
                  'ooooooooooooooooooooooooooooooooooooo.co')
    result = adstxtcrawler._check_viability(bad_domain)
    assert result is False, "Domain validation failed."


def test_query_domains(adstxtcrawler, mocker, caplog):
    caplog.set_level(logging.INFO)

    mock_es = mocker.patch.object(adstxtcrawler.client, 'search')
    mock_es.return_value = {
        'aggregations': {'top_domains': {'buckets': [{'key': 'foo.com'}]}}}

    assert adstxtcrawler._query_for_domains(
        'index', '{"query": true}') == ['foo.com']


def test_broken_fetch_sentry_1023(adstxtcrawler, mocker, caplog):
    caplog.set_level(logging.INFO)
    adstxtcrawler._bootstrap_db()
    adstxtcrawler._check_viability(BROKEN_FETCH_SENTRY_1023.domain)
    # This raises...
    # InternalError: (1267, "Illegal mix of collations (latin1_swedish_ci,IMPLICIT)
    # and (utf8mb4_unicode_ci,COERCIBLE) for operation '='")
    # If the encoding types are set wrong at database creation.
    # TODO: Come up with an example of this breaking.
    adstxtcrawler.process_domain(BROKEN_FETCH_SENTRY_1023)


def test_fetch_domains_file(adstxtcrawler, mocker):
    file_uri = '/blah'
    adstxtcrawler.file = True
    adstxtcrawler.es = False
    adstxtcrawler.file_uri = file_uri
    mock_file = mocker.patch.object(adstxtcrawler, '_fetch_from_file')
    adstxtcrawler.fetch_domains()

    mock_file.assert_called_once_with(file_uri)


def test_fetch_domains_es(adstxtcrawler, mocker):
    es_index = 'foo'
    es_query = 'bar'
    adstxtcrawler.es = True
    adstxtcrawler.file = False
    adstxtcrawler.es_index = es_index
    adstxtcrawler.es_query = es_query
    mock_domains = mocker.patch.object(adstxtcrawler, '_query_for_domains')
    adstxtcrawler.fetch_domains()

    mock_domains.assert_called_once_with(es_index, es_query)


def test_fetch_from_file_rewrites(adstxtcrawler, tmpdir, caplog):
    caplog.set_level(logging.DEBUG)

    first_domain_list = ["reddit.com", "dailymail.co.uk", "ebay.co.uk"]
    domains_file = tmpdir.join("domains.txt")
    domains_file.write('\n'.join(first_domain_list))

    assert adstxtcrawler._fetch_from_file(
        domains_file.strpath) == first_domain_list

    log.debug(domains_file.strpath)

    # We now need to check that any updates are processed.
    second_domain_list = first_domain_list + ['infectiousmedia.com']
    domains_file.write('\n'.join(second_domain_list))

    log.debug('Writing %r', second_domain_list)
    log.debug(domains_file.strpath)
    assert adstxtcrawler._fetch_from_file(
        domains_file.strpath) == second_domain_list


def test_fetch_from_file_glob(adstxtcrawler, tmpdir, caplog):
    caplog.set_level(logging.DEBUG)

    first_domain_list = ["reddit.com", "dailymail.co.uk", "ebay.co.uk"]
    second_domain_list = ["infectiousmedia.com", "facebook.com"]

    domains_file_first = tmpdir.join("domains.1")
    domains_file_first.write('\n'.join(first_domain_list))
    domains_file_second = tmpdir.join("domains.2")
    domains_file_second.write('\n'.join(second_domain_list))

    glob_path = domains_file_first.strpath.split('.')[0] + '*'

    assert adstxtcrawler._fetch_from_file(
        glob_path) == first_domain_list + second_domain_list


def test_deactivate_reactivate(adstxtcrawler, caplog):
    caplog.set_level(logging.DEBUG)
    adstxtcrawler._bootstrap_db()

    correct = FetchResponse(
        'weather.com',
        datetime.datetime(2018, 3, 26, 10, 55, 59, 661410),
        True,
        (
            'amazon-adsystem.com, 1004, DIRECT',
        )
    )
    mistake = FetchResponse(
        'weather.com',
        datetime.datetime(2018, 3, 26, 10, 55, 59, 661410),
        True,
        (
            # Note the aamazon-adsystem...
            'aamazon-adsystem.com, 1004, DIRECT',
        )
    )
    fixed = FetchResponse(
        'weather.com',
        datetime.datetime(2018, 3, 26, 10, 55, 59, 661410),
        True,
        (
            'amazon-adsystem.com, 1004, DIRECT',
        )
    )

    # Populate the domains table.
    adstxtcrawler._check_viability('weather.com')

    session = adstxtcrawler._session()

    adstxtcrawler.process_domain(correct)
    # Assert it's inserted.
    correct_record = session.query(
        models.Record.supplier_domain,
        models.Record.pub_id,
        models.Record.supplier_relationship,
        models.Record.cert_authority,
        models.Record.active).one_or_none()
    logging.info(correct_record)
    assert correct_record.active is True

    adstxtcrawler.process_domain(mistake)
    # Assert it's now inactive.
    mistake_record = session.query(
        models.Record.supplier_domain,
        models.Record.pub_id,
        models.Record.supplier_relationship,
        models.Record.cert_authority,
        models.Record.active).filter_by(
            supplier_domain='amazon-adsystem.com'
    ).one_or_none()
    logging.info(mistake_record)
    # Record should be disable now.
    assert mistake_record.active is False

    adstxtcrawler.process_domain(fixed)
    # Assert it's reactivated.
    fixed_record = session.query(
        models.Record.supplier_domain,
        models.Record.pub_id,
        models.Record.supplier_relationship,
        models.Record.cert_authority,
        models.Record.active).filter_by(
            supplier_domain='amazon-adsystem.com'
    ).one_or_none()
    logging.info(fixed_record)
    # Check that's again set to active.
    assert fixed_record.active is True


def test_deactivate_reactivate_new_cert_authority(adstxtcrawler, caplog):
    caplog.set_level(logging.DEBUG)
    adstxtcrawler._bootstrap_db()

    correct = FetchResponse(
        'weather.com',
        datetime.datetime(2018, 3, 26, 10, 55, 59, 661410),
        True,
        (
            'amazon-adsystem.com, 1004, DIRECT',
        )
    )
    mistake = FetchResponse(
        'weather.com',
        datetime.datetime(2018, 3, 26, 10, 55, 59, 661410),
        True,
        (
            # amazon-adsystem has been removed so old record should be inactive
            'blah.com, 1111, DIRECT',
        )
    )
    fixed = FetchResponse(
        'weather.com',
        datetime.datetime(2018, 3, 26, 10, 55, 59, 661410),
        True,
        (
            # New record with new cert_authority.
            'amazon-adsystem.com, 1004, DIRECT, asdasdasd',
        )
    )

    # Populate the domains table.
    adstxtcrawler._check_viability('weather.com')

    session = adstxtcrawler._session()

    adstxtcrawler.process_domain(correct)
    # Assert it's inserted.
    correct_record = session.query(
        models.Record.supplier_domain,
        models.Record.pub_id,
        models.Record.supplier_relationship,
        models.Record.cert_authority,
        models.Record.active).one()
    logging.info('Correct Record: %r.', correct_record)
    assert correct_record.active is True

    adstxtcrawler.process_domain(mistake)
    # Assert it's now inactive.
    mistake_record = session.query(
        models.Record.supplier_domain,
        models.Record.pub_id,
        models.Record.supplier_relationship,
        models.Record.cert_authority,
        models.Record.active).filter_by(
            supplier_domain='amazon-adsystem.com'
    ).one()
    logging.info('Mistake record: %r.', mistake_record)
    # Record should be disable now.
    assert mistake_record.active is False

    # Reprocess the domain list and check the new record is there as
    # well as the old record still being inactive.
    adstxtcrawler.process_domain(fixed)
    # Assert it's reactivated.
    fixed_record = session.query(
        models.Record.supplier_domain,
        models.Record.pub_id,
        models.Record.supplier_relationship,
        models.Record.cert_authority,
        models.Record.active).filter_by(
            supplier_domain='amazon-adsystem.com',
            cert_authority='asdasdasd'
    ).one()
    logging.info('Fixed record: %r.', fixed_record)
    # Check that's again set to active.
    assert fixed_record.active is True

    fixed_record_inactive = session.query(
        models.Record.supplier_domain,
        models.Record.pub_id,
        models.Record.supplier_relationship,
        models.Record.cert_authority,
        models.Record.active).filter_by(
            supplier_domain='amazon-adsystem.com',
            cert_authority=None
    ).one()
    logging.info('Fixed inactive record: %r.', mistake_record)
    assert fixed_record_inactive.active is False
