import asyncio
import datetime
import json
import logging
import queue
import threading
from typing import List

from elasticsearch import Elasticsearch

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from sqlalchemy.orm import sessionmaker
import validators

import adstxt.fetch as fetch
import adstxt.models as models
import adstxt.transform as transform


LOG = logging.getLogger(__name__)


class AdsTxtCrawler:

    def __init__(self,
                 es,
                 file,
                 db_uri,
                 es_uri=None,
                 es_query=None,
                 es_index=None,
                 file_uri=None,
                 crawler_id=None):
        self.es = es
        self.file = file
        self.db_uri = db_uri
        self.es_uri = es_uri
        self.es_query = es_query
        self.es_index = es_index
        self.file_uri = file_uri
        self.crawler_id = crawler_id
        self._session = sessionmaker()
        self._testing = False
        self.es = Elasticsearch(self.es_uri)

    def _get_engine(self):
        if 'mysql+pymysql' in self.db_uri:
            connect_args = {'init_command': "SET @@collation_connection"
                                            "='utf8mb4_unicode_ci'"}
            LOG.info('Using connection string of %r and args of %r',
                     self.db_uri, connect_args)
            return create_engine(self.db_uri,
                                 pool_size=40,
                                 connect_args=connect_args)
        else:
            LOG.info('Using local sqllite.')
            connect_args = {}
            return create_engine(self.db_uri,
                                 connect_args=connect_args)

    def _bootstrap_db(self):
        """Bootstrap the data stores, and create a shared session
        object with the engine setup."""
        self.engine = self._get_engine()

        # Generate a root session object, this is configured locally
        # and then shared.  Each user of this will then scope their
        # own session.
        session = sessionmaker()
        session.configure(bind=self.engine)
        LOG.debug('Building tables.')
        models.Base.metadata.create_all(self.engine)
        LOG.debug('Built database.')
        # Share our root session object.
        self._session = session

    def _last_updated_at(self, domain: str) -> datetime.datetime:
        session = self._session()

        # Check to see if the domain is present in the domains table.
        db_domain = session.query(
            models.Domain).filter_by(name=domain).first()

        if not db_domain:
            # Write it with a min time so we update it this first session.
            db_domain = models.Domain(name=domain,
                                      last_updated=datetime.datetime.min)
            session.add(db_domain)
            session.commit()

        # Return last updated at time.
        return db_domain.last_updated

    def _check_viability(self, domain: str) -> bool:
        """Check to see if a domain is viable to be crawled.

        Basic validation goes on here, first we assert that the domain
        is infact a valid domain, with a TLD and more info.  From here we
        check to see if we've crawled the domain recently.datetime

        Args:
            domain (str): domain to precheck.

        Returns:
            bool: Truthy if a domain is viable to be scanned again,
                  Falsy if the domain has already been scanned or
                  does not pass validation.
        """
        if not validators.domain(domain):
            LOG.info('%r found to be an invalid domain.', domain)
            return False

        # Check to see if the domain is present in the domains table.
        last_updated = self._last_updated_at(domain)

        # Check to see when we last updated the domains data.
        # If the last updated time was greater than an six hours ago, check.
        # TODO: We should get the cache control headers back off the page
        # and use that instead.  Set as 6 hours for the moment.
        if (datetime.datetime.utcnow() -
                last_updated < datetime.timedelta(minutes=360)):
            # Skip to next domain as this ones got new data.
            LOG.debug('Skipping %r domain due to recent update at %r',
                      domain, last_updated)
            return False

        return True

    def process_domain(self, fetchdata: fetch.FetchResponse) -> None:
        """Process a domains FetchResponse into inserted records and variables.

        Pipeline roughly goes as follows.
        1. Check FetchResponse data is valid, if not update scraped_at
            and return.  If it is valid, update the db_domain details we have.
        2. Iterate through response tuple, checking what's currently in the
            database so we don't insert duplicate records.
        3. Try to commit
        Args:
            fetchdata (FetchResponse): Named tuple of fetch data.

        Returns:
            None
        """
        # Setup a new SQL session.
        session = self._session(bind=self.engine)

        # Fetch domain from database. This should always exist and will
        # raise an sqlalchemy.orm.exc.NoResultFound if nothing is found.
        db_domain = session.query(
            models.Domain).filter_by(name=fetchdata.domain).one()

        LOG.debug('Processing fetchdata for %r', fetchdata.domain)
        LOG.debug('Using %r as db_domain.', db_domain)

        # If we've got bad data from an endpoint, log this and return.
        if not fetchdata.response or not fetchdata.adstxt_present:
            # TODO: Passback more debug data on failure from fetches.
            LOG.debug('Bad AdsTxt file found, updating TTLs and returning.')
            # Update the last updated at row so we don't try and
            # update the record again too soon.
            db_domain.last_updated = fetchdata.scraped_at
            # This is set to null at creation, explicitly set to False as we
            # know that there is not one now.
            db_domain.adstxt_present = False
            session.add(db_domain)
            session.commit()
            return
        # Else we've got a valid record from Fetch.  Update the db_domain
        # details we hold locally but don't commit until the end.
        else:
            db_domain.last_updated = fetchdata.scraped_at
            db_domain.adstxt_present = True
            session.add(db_domain)

        # We want to look back and verify that all of these exist.
        processed_records = []
        for row in fetchdata.response:
            # Transform the rows and add them to a list to validate against.
            processed_row = transform.process_row(row)

            # Check to see what the row is returning and process.
            if isinstance(processed_row, transform.AdsRecord):

                # Keep a list of records to compare back with.
                processed_records.append(processed_row)

                # Check for presence of record in existing Record table.
                # If if does then skip to the next record.
                try:
                    record_exists = session.query(
                        models.Record).filter_by(
                            domain=db_domain,
                            supplier_domain=processed_row.supplier_domain,
                            pub_id=processed_row.pub_id,
                            supplier_relationship=processed_row.supplier_relationship,
                            cert_authority=processed_row.cert_authority
                    ).one_or_none()

                # Something in the query was bad. Skip to the next record.
                except SQLAlchemyError as excpt:
                    LOG.exception('Unprocessible row. %r is bad due to %r',
                                  processed_row, excpt)
                    continue

                # If the record isn't present insert with fetchdata.
                if not record_exists:
                    db_record = models.Record(
                        domain_id=db_domain.id,
                        supplier_domain=processed_row.supplier_domain,
                        pub_id=processed_row.pub_id,
                        supplier_relationship=processed_row.supplier_relationship,
                        cert_authority=processed_row.cert_authority,
                        first_seen=fetchdata.scraped_at,
                        active=True)
                    LOG.debug('Adding new record to database, %r', db_record)
                    try:
                        session.add(db_record)
                    except DBAPIError:
                        LOG.error('Unable to insert... %r', db_record)
                # If the record does exist check to ensure it's active.
                else:
                    # It's not active so reactivate the record.
                    if not record_exists.active:
                        record_exists.active = True
                        session.commit()
                        LOG.debug(
                            'Record was found to be inactive, reactivating...')

            elif isinstance(processed_row, transform.AdsVariable):
                # Check for presence of variable in Variable table.
                # If it does then skip to next record.
                variable_exists = session.query(
                    models.Variable).filter_by(
                        domain=db_domain,
                        key=processed_row.key).first()

                if not variable_exists:
                    LOG.debug('New variable %r inserted for %r',
                              db_domain.name, processed_row.key)
                    db_variable = models.Variable(
                        domain_id=db_domain.id,
                        key=processed_row.key,
                        value=processed_row.value)
                    session.add(db_variable)
                elif variable_exists.value != processed_row.value:
                    LOG.debug('Key %r for %r has been updated.',
                              variable_exists.key, db_domain.name)
                    variable_exists.value = processed_row.value
                    session.add(variable_exists)
                else:
                    # Check is there and is up to date.
                    continue
            # Else it's nil, skip to next record.
            else:
                continue

        # Validate that evereything in the records table is also in our list
        # of processed rows.  Run through the record table then variables.
        active_records = session.query(
            models.Record.supplier_domain,
            models.Record.pub_id,
            models.Record.supplier_relationship,
            models.Record.cert_authority).filter_by(
                domain_id=db_domain.id, active=True).all()

        # Find what's in active_records but is not in processed_records.
        active_records_not_seen = set(active_records).difference(
            set(processed_records))
        # Set all of these records as inactive.
        for record in active_records_not_seen:
            LOG.debug('%r was found to be inactive.', record)
            session.query(
                models.Record).filter_by(
                    domain_id=db_domain.id,
                    supplier_domain=record.supplier_domain,
                    pub_id=record.pub_id,
                    supplier_relationship=record.supplier_relationship,
                    cert_authority=record.cert_authority).one().active = False

        # Domain is completely processed at this point.  Commit all records.
        session.commit()
        LOG.debug('Session commited and domain processed.')

    def fetch_domains(self) -> List[str]:
        if self.file:
            return self._fetch_from_file(self.file_uri)
        else:
            return self._query_for_domains(self.es_index, self.es_query)

    def _fetch_from_file(self, path) -> List[str]:
        with open(path, 'r') as f:
            domain_file = f.read()

        domains = []
        for row in domain_file.split('\n'):
            if row:
                domains.append(row)

        return domains

    def _query_for_domains(self, index, body) -> List[str]:
        query = json.loads(body)
        res = self.es.search(index=index, body=query)

        # Return just the domains.
        domains = [i['key'] for i
                   in res['aggregations']['top_domains']['buckets']]
        LOG.debug('Fetched total %s domains from ES.', len(domains))

        return domains

    def _run_once(self) -> None:
        """Query for domains and insert into database.

        Pipeline works as follows, we query for domains and check their
        viability for searching.  We setup a worker thread which processes
        fetched results in the background, while in the foreground we generate
        a list of futures which fetch using aiohttp/asyncio and are gathered.
        These populate a background queue which processes all of these events
        until the queue has had all items processed.

        It would be best if we used async callbacks or similar that then
        updated the database once a fetch was done, this however requires
        a working asyncio/mysql/sqlalchemy wrapper.  Until that exists or we
        can spend the time working on one this pattern is the best we can do.

        Args:
            None

        Returns:
            None

        ATTENTION: This requires databases and connections to be
        bootstrapped.  If you're manually running this please call
        self._bootstrap_db as well.
        """
        # Query for domains and filter to see if theyt're checkable.
        domains = [x for x in self.fetch_domains()
                   if self._check_viability(x)]

        def worker():
            while True:
                # Get a fetch event from the Queue and write to DB.
                fetch_event = fetch_queue.get(block=True)
                # Check to see if the sentinel value has been pushed in.
                if fetch_event is None:
                    # Let's break out of this loop and exit the function.
                    break
                # Catch the top level exception and continue onto the next
                # record.
                try:
                    self.process_domain(fetch_event)
                except Exception as e:
                    LOG.exception(e)
                    pass
                # Ack that event as being done.
                fetch_queue.task_done()
                # Log this event as being processed.
                LOG.debug('Task done %r', fetch_event)

        # Setup a Queue and worker for processing fetch events.fetch
        fetch_queue = queue.Queue()  # type: queue.Queue
        thread = threading.Thread(target=worker)
        thread.start()

        # Most of what we're doing here is waiting on network IO of some kind.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def fetcher(domain):
            try:
                fetch_event = await fetch.fetch(domain, self.crawler_id)
            # Just crush exceptions here
            except Exception:
                pass
            else:
                fetch_queue.put(fetch_event)

        # Setup a list of function calls.
        fetches = [fetcher(x) for x in domains]
        # This could potentially grow to be very large.
        loop.run_until_complete(asyncio.gather(*fetches))

        # Close the loop once we're done.
        loop.close()

        # Block until all tasks are done.
        fetch_queue.join()
        # Add our sentinel value so the worker quits it's loop.
        fetch_queue.put(None)
        # Close thread now we're done writing to the database.
        thread.join()

    def run(self) -> None:
        LOG.info('Starting adstxt crawler...')

        self._bootstrap_db()
        LOG.info('Databases bootstrapped...')

        while True:
            LOG.info('Searching for domains to crawl...')
            self._run_once()
            LOG.info('Done processing current available domains.')
            # There's no sleeping as this process takes ages.
            # Just loop round and update anything that's required.
