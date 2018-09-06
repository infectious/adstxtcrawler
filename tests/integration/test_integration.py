import datetime
import json
import os
import logging
import time

import pytest
from elasticsearch import Elasticsearch

import adstxt.main as main
import adstxt.models as models
from adstxt.fetch import FetchResponse

TEST_DOMAINS = ['ebay.co.uk',
                'reddit.com',
                'dailymail.co.uk']
USER_AGENT = 'adstxt_integration_test'

ES_QUERY = {
    "query": {
        "bool": {
            "must": {
                "term": {
                    "event.keyword": "debug"
                }
            },
            "filter": {
                "range": {
                    "ts": {
                        "gte": "now-1h/h",
                        "lte": "now"
                    }
                }
            }
        }
    },
    "aggs": {
        "top_domains": {
            "terms": {
                "field": "domain.keyword",
                "size": 10000
            }
        }
    }, "size": 0
}


@pytest.fixture
def adstxt_integration(mocker, caplog, tmpdir, request):
    caplog.set_level(logging.INFO)

    idb_test_index_for_es = 'idb-testing'

    logging.info('Starting integration testing...')
    # If we're running in Drone.  Use real endpoints.
    if os.environ.get('CI'):
        # Write some test data to Elasticsearch.
        es_uri = 'elasticsearch'
        es = Elasticsearch(es_uri)
        logging.info('Setup ES, populating with data...')
        for domain in TEST_DOMAINS:
            doc = {'event': 'debug',
                   'domain': domain,
                   'ts': datetime.datetime.utcnow().isoformat()}
            es.index(index=idb_test_index_for_es, doc_type='event', body=doc)
        logging.info('ES population completed.')
        # Sleep a while to let ES digest that...
        # This is greater than the index refresh interval.
        time.sleep(10)
        # Setup crawler using Drone database.
        # This is from http://docs.drone.io/mysql-example/
        db_uri = ('mysql+pymysql://root:root@'
                  'database:3306/adstxt?charset=utf8mb4')

        crawler = main.AdsTxtCrawler(True,
                                     False,
                                     db_uri,
                                     es_uri=es_uri,
                                     es_query=json.dumps(ES_QUERY),
                                     es_index=idb_test_index_for_es,
                                     crawler_id=USER_AGENT)
    # If this isn't true, then setup something we can run locally.
    # This won't be as good as it won't use MySQL, but should be 80%
    # of the way there.  CI should catch sqlite/mysql differences.
    else:
        temp_sql_file = tmpdir.join("db.sqlite")
        db_uri = "sqlite:///" + temp_sql_file.strpath

        os.system('touch ' + temp_sql_file.strpath)
        logging.info('Using %r as connection.', db_uri)

        # This ES uri is mocked out so don't worry about it.
        crawler = main.AdsTxtCrawler(True,
                                     False,
                                     db_uri,
                                     es_uri='localhost',
                                     es_query=json.dumps(ES_QUERY),
                                     es_index=idb_test_index_for_es,
                                     crawler_id=USER_AGENT)
        mock_es = mocker.patch.object(crawler, '_query_for_domains')
        mock_es.return_value = TEST_DOMAINS

    # Set testing to True so we don't run on forever.
    crawler._testing = True

    # We have to bootstrap the db to get an engine object,
    # once we do we can drop everything to ensure we start from
    # a clean empty database.
    crawler._bootstrap_db()
    models.Base.metadata.drop_all(crawler.engine)
    # Bootstrap a database for everyone.
    crawler._bootstrap_db()

    def gc():
        logging.info("Clearing all datastores.")
        models.Base.metadata.drop_all(crawler.engine)
        # Drop whatever is in ES.
        if os.environ.get('CI'):
            es.indices.delete(index=idb_test_index_for_es, ignore=[400, 404])

        crawler._session.close_all()
        logging.info("Done all cleanup.")

    # GC everything up once we're done with the fixture.
    request.addfinalizer(gc)

    return crawler


@pytest.mark.integration
def test_integration_run_once(caplog, adstxt_integration):
    """Run an integration test, mocking and stubbing out as little
    as possible.  Depending upon if we're running locally or in the CI
    server we'll have more faux resources available to us.
    """
    caplog.set_level(logging.INFO)

    adstxt_integration._run_once()

    session = adstxt_integration._session()
    domains = session.query(models.Domain).all()
    records = session.query(models.Record).all()

    assert len(domains) == 3
    # There's a lot of records here.
    assert len(records) >= 100

    # Check some individual records to see how they're doing.
    ebay = session.query(models.Domain).filter_by(
        name='ebay.co.uk').first()
    assert ebay.adstxt_present is True

    # Check to see we last updated independent a minute ago.
    assert (datetime.datetime.utcnow() - ebay.last_updated >
            datetime.timedelta(minutes=-1))

    # Check to see if we've got multiple pub ids.
    assert session.query(models.Record.pub_id).group_by(
        models.Record.pub_id).count() > 10

    # Clean up databases at the end here.
    adstxt_integration._session.close_all()


@pytest.mark.integration
def test_integration_verify_process_domain_gc(caplog, adstxt_integration):
    """Run an integration test, specifically looking at how we're storing
    data in the database.  We're just interacting with process_domain here."""
    caplog.set_level(logging.DEBUG)

    domain = 'blah.com'
    scraped_at = datetime.datetime.utcnow()
    adstxt_present = True
    records = ('adform.com, 1616, DIRECT',
               'infectiousmedia.com, 666, DIRECT',
               'infectiousmedia.com, 999, RESELLER')

    dummy_fetch = FetchResponse(domain=domain,
                                scraped_at=scraped_at,
                                adstxt_present=adstxt_present,
                                response=records)

    # We need to insert the domain into the database initially.
    # This checks to see if it's present and if not adds it with empty values.
    adstxt_integration._last_updated_at(domain)

    # Now we can insert all of the records present in the dummy fetch.
    adstxt_integration.process_domain(dummy_fetch)

    # Now we've inserted some records, we can add another fetch response which
    # doesn't have infectiousmedia domains present.
    dummy_fetch_sans_infectious = FetchResponse(
        domain=domain,
        scraped_at=scraped_at,
        adstxt_present=adstxt_present,
        response=records[:1])
    adstxt_integration.process_domain(dummy_fetch_sans_infectious)

    # Verify that the infectious domains are both inactive.
    session = adstxt_integration._session()
    records = session.query(
        models.Record).filter_by(
            domain_id=1, active=False).all()

    assert len(records) == 2

