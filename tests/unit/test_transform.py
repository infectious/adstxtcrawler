import adstxt.transform as transform


DUMMY_FETCH_ROW_COMMENT = "# Independent.co.uk (ESI Media) ads.txt [DEC 2017]"

DUMMY_FETCH_ROW_1 = "adtech.com, 10217, RESELLER"
DUMMY_FETCH_ROW_1_EXPECTED = transform.AdsRecord(
    supplier_domain='adtech.com',
    pub_id='10217',
    supplier_relationship='reseller',
    cert_authority=None)
DUMMY_FETCH_ROW_2 = "advertising.com, 10316, RESELLER, 7842df1d2fe2db34"
DUMMY_FETCH_ROW_2_EXPECTED = transform.AdsRecord(
    supplier_domain='advertising.com',
    pub_id='10316',
    supplier_relationship='reseller',
    cert_authority='7842df1d2fe2db34')

DUMMY_FETCH_ROW_2_COMMENT = ("advertising.com, 10316, RESELLER"
                             ", 7842df1d2fe2db34 # basd foo")
DUMMY_SHORT_ROW_END_COMMENT = "advertising.com, 10316, RESELLER # reseller foo"
DUMMY_SHORT_BAD_RESELLER = "advertising.com, 10316, reseller tv # reseller tv"
DUMMY_SHORT_BAD_SUPPLIER = "advertising.com, 10316, direct tv # direct tv blah"
DUMMY_SHORT_ROW_EXPECTED = transform.AdsRecord(
    supplier_domain='advertising.com',
    pub_id='10316',
    supplier_relationship='reseller',
    cert_authority=None)
DUMMY_SHORT_ROW_SUPPLIER_EXPECTED = transform.AdsRecord(
    supplier_domain='advertising.com',
    pub_id='10316',
    supplier_relationship='direct',
    cert_authority=None)
DUMMY_FETCH_VARIABLE_1 = "contact=programmatic.platforms@assocnews.co.uk"
DUMMY_FETCH_VARIABLE_1_EXPECTED = transform.AdsVariable(
    key='contact', value='programmatic.platforms@assocnews.co.uk')
DUMMY_FETCH_VARIABLE_2 = "foo=this is an=annoying string"
DUMMY_FETCH_VARIABLE_2_EXPECTED = transform.AdsVariable(
    key='foo', value='this is an=annoying string')

MIXED_CASE_PUBID = "advertising.com, 78AbC123DeFasGFG, RESELLER"


def test_process_row_comments():
    # Check comments.
    assert transform.process_row(DUMMY_FETCH_ROW_COMMENT) is None

    assert transform.process_row(
        DUMMY_FETCH_ROW_2_COMMENT) == DUMMY_FETCH_ROW_2_EXPECTED
    assert transform.process_row(
        DUMMY_SHORT_ROW_END_COMMENT) == DUMMY_SHORT_ROW_EXPECTED
    assert transform.process_row(
        DUMMY_SHORT_BAD_RESELLER) == DUMMY_SHORT_ROW_EXPECTED
    assert transform.process_row(
        DUMMY_SHORT_BAD_RESELLER) == DUMMY_SHORT_ROW_EXPECTED


def test_process_records():
    # Check records.
    assert transform.process_row(
        DUMMY_FETCH_ROW_1) == DUMMY_FETCH_ROW_1_EXPECTED
    assert transform.process_row(
        DUMMY_FETCH_ROW_2) == DUMMY_FETCH_ROW_2_EXPECTED
    assert transform.process_row(
        MIXED_CASE_PUBID) == transform.AdsRecord(
            supplier_domain='advertising.com',
            pub_id='78AbC123DeFasGFG',
            supplier_relationship='reseller',
            cert_authority=None)


def test_process_row_variables():
    # Check variables.
    assert transform.process_row(
        DUMMY_FETCH_VARIABLE_1) == DUMMY_FETCH_VARIABLE_1_EXPECTED
    assert transform.process_row(
        DUMMY_FETCH_VARIABLE_2) == DUMMY_FETCH_VARIABLE_2_EXPECTED


def test_return_row_too_small():
    assert transform.process_row(
        "foo.com, 1231312") is None, "Assert too small rows are invalid."


def test_reseller_direct_none_extraction():
    reseller_caps = "foo, 123123, RESELLER"
    reseller_lower = "foo, 123123, reseller"

    direct_caps = "foo, 123123, DIRECT"
    direct_lower = "foo, 123123, direct"

    other_stuff = "foo, 123123, blah"

    assert transform.process_row(
        reseller_caps).supplier_relationship == 'reseller'
    assert transform.process_row(
        reseller_lower).supplier_relationship == 'reseller'

    assert transform.process_row(
        direct_caps).supplier_relationship == 'direct'
    assert transform.process_row(
        direct_lower).supplier_relationship == 'direct'

    assert transform.process_row(
        other_stuff) is None
