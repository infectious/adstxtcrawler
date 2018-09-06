import logging
from typing import NamedTuple, Optional, Union


LOG = logging.getLogger(__name__)


class AdsRecord(NamedTuple):
    supplier_domain: str
    pub_id: str
    supplier_relationship: str
    cert_authority: Optional[str]


class AdsVariable(NamedTuple):
    key: str
    value: str


def process_row(row: str) -> Union[AdsRecord, AdsVariable, None]:
    """Process a ads.txt row and return a tuple of data.

    Args:
        row (str): Raw string from the crawler to be processed.

    Returns:
        Union[AdsRecord, AdsVariable, None]: Depending upon row.

    This just follows the adstxt spec.
    """

    # Filter out comments.
    if row.startswith('#'):
        return None

    # If the row has a '#' that's probably an inline comment.
    if '#' in row:
        # Mutate row if there's an inline comment in it.
        row = row.split('#')[0]

    # If a row contains an equals then it's a variable.
    # TODO: Remove this hack to get around bad switch contepts vars.
    if '=' in row and 'concepts' not in row:
        # The value can contain arbitrary text, so find the first
        # equals to split on.
        for pos, char in enumerate(row):
            if char == '=':
                split_pos = pos
                break
        # Split the string based off the index position.
        key = row[:split_pos]
        # We don't want to include the seperator here.
        value = row[split_pos + 1:]

        return AdsVariable(key=key, value=value)

    #  In this case it might be a record.
    #  Filter out based upon 3.4.2 THE RECORD.
    #  Remove all whitespace.
    LOG.debug('Processing a Record, %r', row)

    # Strip tabs and spaces.
    clean_row = row.strip(' ').strip('\t')
    record_vals = clean_row.split(',')

    # If it's less than 3 it's not a proper record.  Exit.
    if len(record_vals) < 3:
        LOG.debug('Bad record found, %r', row)
        return None

    # Domain names are case insensitive so lowercase.
    supplier_domain = record_vals[0].lower().strip().strip('\t')
    # Pub ID shows this as a string or int.
    pub_id = record_vals[1].strip().strip('\t')

    # This can only be one of two values, try and extract that.
    relationship = record_vals[2].lower().strip().strip('\t')

    if 'reseller' in relationship:
        supplier_relationship = 'reseller'
    elif 'direct' in relationship:
        supplier_relationship = 'direct'
    else:
        LOG.debug('Found a bad record; %s', row)
        return None

    # Cert authority is optional.
    if len(record_vals) == 4:
        cert_authority = record_vals[3].strip()
    # mypy trips up on sqlalchemy, nullable=True so just ignore it.
    else:
        cert_authority = None  # type: ignore

    ret_val = AdsRecord(supplier_domain=supplier_domain,
                        pub_id=pub_id,
                        supplier_relationship=supplier_relationship,
                        cert_authority=cert_authority)

    LOG.debug('Returning record... %r', ret_val)
    return ret_val
