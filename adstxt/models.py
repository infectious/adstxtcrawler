from typing import Any

from sqlalchemy import (
    Column, ForeignKey, Integer, String, DateTime, Boolean, Text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()  # type: Any


class Domain(Base):
    __tablename__ = 'domains'

    id = Column(Integer, primary_key=True)
    #  Full domain records may not exceed 255 chars.
    #  https://tools.ietf.org/html/rfc1034#section-3.1
    name = Column(String(255), nullable=False)
    last_updated = Column(DateTime)
    adstxt_present = Column(Boolean, nullable=True)

    def __repr__(self):  # pragma: no cover
        return ("<Domain(name='%s', last_updated='%s',"
                "adstxt_present='%s')>") % (
            self.name, self.last_updated, self.adstxt_present)


class Record(Base):
    __tablename__ = 'records'

    id = Column(Integer, primary_key=True)
    # Parent domain foreign key.
    domain_id = Column(Integer, ForeignKey('domains.id'))
    domain = relationship(Domain)
    # Adstxt record fields.
    supplier_domain = Column(String(255), nullable=False)
    pub_id = Column(String(255), nullable=False)
    supplier_relationship = Column(String(30), nullable=False)
    cert_authority = Column(String(255), nullable=True)
    # Keep track of the records state.
    first_seen = Column(DateTime, nullable=True)
    active = Column(Boolean, nullable=True)

    def __repr__(self):  # pragma: no cover
        return ("<Record(domain_id='%s', supplier_domain='%s', "
                "pub_id='%s', supplier_relationship='%s', "
                "cert_authority='%s', first_seen='%s', active='%s')>") % (
            self.domain_id, self.supplier_domain, self.pub_id,
            self.supplier_relationship, self.cert_authority,
            self.first_seen, self.active)


class Variable(Base):

    __tablename__ = 'variables'
    id = Column(Integer, primary_key=True)
    # Parent domain foreign key.
    domain_id = Column(Integer, ForeignKey('domains.id'))
    domain = relationship(Domain)
    # Adstxt variable fields.
    key = Column(String(255), nullable=False)
    # Value can be arbitrarily long.
    value = Column(Text, nullable=False)

    def __repr__(self):  # pragma: no cover
        return "<Variable(domain='%s', key='%s', value='%s')>" % (
            self.domain, self.key, self.value)
