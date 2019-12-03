#  Polkascan PRE Harvester
#
#  Copyright 2018-2019 openAware BV (NL).
#  This file is part of Polkascan.
#
#  Polkascan is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Polkascan is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Polkascan. If not, see <http://www.gnu.org/licenses/>.
#
#  settings.py

import os

DB_NAME = os.environ.get("DB_NAME", "polkascan")
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = os.environ.get("DB_PORT", 33061)
DB_USERNAME = os.environ.get("DB_USERNAME", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "root")

DB_CONNECTION = os.environ.get("DB_CONNECTION", "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
    DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
))

SUBSTRATE_RPC_URL = os.environ.get("SUBSTRATE_RPC_URL", "http://18.138.185.139:9933/")
SUBSTRATE_ADDRESS_TYPE = int(os.environ.get("SUBSTRATE_ADDRESS_TYPE", 42))
HRP = os.environ.get("HRP", "tyee")
# Simulate Scale encoded extrinsics per block for e.g. performance tests
# Example:
# SUBSTRATE_MOCK_EXTRINSICS = ["0xa50383ff76729e17ad31469debcb60f3ce3622f79143e442e77b58d6e2195d9ea998680d283c1715298aada424241284e4c3d2bec57a8b89e1bfa5502c0f84866cb94f64b666c04ceb88b7274612fea6bcdf7683701b96c13264d5326ecdcd5661df5502d500080008000f69590e7c83f3b71826537aff19ce9d173efeb887cca69c02b991f6ca75a8f43e05e5ef718a29d168e8df39367398cc60b9b45c7815fb2bfa362693a281676e1c7e66ad780b39e767f22efe0065929db7c69cef006d69a0ea8739c22fa1a06cf257d1cc14c340bdf2944ba8615b2a32cdc5774c9f93af6ef7eb3eab07caf94f00"] * 5000
SUBSTRATE_MOCK_EXTRINSICS = os.environ.get("SUBSTRATE_MOCK_EXTRINSICS", None)

TYPE_REGISTRY = os.environ.get("TYPE_REGISTRY", "default")

FINALIZATION_BY_BLOCK_CONFIRMATIONS = int(os.environ.get("FINALIZATION_BY_BLOCK_CONFIRMATIONS", 0))

DEBUG = bool(os.environ.get("DEBUG", False))

# Version compatibility switches

LEGACY_SESSION_VALIDATOR_LOOKUP = bool(os.environ.get("LEGACY_SESSION_VALIDATOR_LOOKUP", False))


SHARDS_TABLE = {"shard.0": "http://18.138.185.139:9933/", "shard.1": "http://3.1.239.198:9933/",
                "shard.2": "http://52.221.95.128:9933/", "shard.3": "http://13.229.27.44:9933/"}


NUM = {"http://18.138.185.139:9933/": "0", "http://3.1.239.198:9933/": "1", "http://52.221.95.128:9933/": "2",
       "http://13.229.27.44:9933/": "3"}
# Constants

ACCOUNT_AUDIT_TYPE_NEW = 1
ACCOUNT_AUDIT_TYPE_REAPED = 2

ACCOUNT_INDEX_AUDIT_TYPE_NEW = 1
ACCOUNT_INDEX_AUDIT_TYPE_REAPED = 2

DEMOCRACY_PROPOSAL_AUDIT_TYPE_PROPOSED = 1
DEMOCRACY_PROPOSAL_AUDIT_TYPE_TABLED = 2

DEMOCRACY_REFERENDUM_AUDIT_TYPE_STARTED = 1
DEMOCRACY_REFERENDUM_AUDIT_TYPE_PASSED = 2
DEMOCRACY_REFERENDUM_AUDIT_TYPE_NOTPASSED = 3
DEMOCRACY_REFERENDUM_AUDIT_TYPE_CANCELLED = 4
DEMOCRACY_REFERENDUM_AUDIT_TYPE_EXECUTED = 5

DEMOCRACY_VOTE_AUDIT_TYPE_NORMAL = 1
DEMOCRACY_VOTE_AUDIT_TYPE_PROXY = 2

try:
    from app.local_settings import *
except ImportError:
    pass
