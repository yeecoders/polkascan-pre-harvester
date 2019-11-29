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
#  event.py
#
from packaging import version

from app.models.data import Account, AccountIndex, AccountAudit, \
    AccountIndexAudit, RuntimeStorage
from app.processors.base import EventProcessor
from app.settings import ACCOUNT_AUDIT_TYPE_NEW, ACCOUNT_AUDIT_TYPE_REAPED, ACCOUNT_INDEX_AUDIT_TYPE_NEW, \
    ACCOUNT_INDEX_AUDIT_TYPE_REAPED, DEMOCRACY_PROPOSAL_AUDIT_TYPE_PROPOSED, DEMOCRACY_PROPOSAL_AUDIT_TYPE_TABLED, \
    SUBSTRATE_RPC_URL, DEMOCRACY_REFERENDUM_AUDIT_TYPE_STARTED, DEMOCRACY_REFERENDUM_AUDIT_TYPE_PASSED, \
    DEMOCRACY_REFERENDUM_AUDIT_TYPE_NOTPASSED, DEMOCRACY_REFERENDUM_AUDIT_TYPE_CANCELLED, \
    DEMOCRACY_REFERENDUM_AUDIT_TYPE_EXECUTED, LEGACY_SESSION_VALIDATOR_LOOKUP
from app.utils.ss58 import ss58_encode
from scalecodec import ScaleBytes
from scalecodec.base import ScaleDecoder
from scalecodec.exceptions import RemainingScaleBytesNotEmptyException
from substrateinterface import SubstrateInterface
from app.processors.block import *


class NewAccountEventProcessor(EventProcessor):
    module_id = 'balances'
    event_id = 'NewAccount'

    def accumulation_hook(self, db_session):

        # Check event requirements
        if len(self.event.attributes) == 2 and \
                self.event.attributes[0]['type'] == 'AccountId' and self.event.attributes[1]['type'] == 'Balance':

            account_id = self.event.attributes[0]['value'].replace('0x', '')
            balance = self.event.attributes[1]['value']

            self.block._accounts_new.append(account_id)

            account_audit = AccountAudit(
                account_id=account_id,
                block_id=self.event.block_id,
                extrinsic_idx=self.event.extrinsic_idx,
                event_idx=self.event.event_idx,
                type_id=ACCOUNT_AUDIT_TYPE_NEW
            )

            account_total = Account.query(db_session).filter_by(
                id=account_audit.account_id).count()
            print('NewAccountEventProcessor get account_total {} =='.format(account_total))

            if account_total <= 0:
                account = Account(
                    id=account_audit.account_id,
                    address=ss58_encode(account_audit.account_id, SUBSTRATE_ADDRESS_TYPE),
                    created_at_block=account_audit.block_id,
                    updated_at_block=account_audit.block_id,
                    balance=0
                )
                if account_audit.type_id == ACCOUNT_AUDIT_TYPE_REAPED:
                    account.count_reaped += 1
                    account.is_reaped = True

                elif account_audit.type_id == ACCOUNT_AUDIT_TYPE_NEW:
                    account.is_reaped = False

                account.updated_at_block = account_audit.block_id
                # If reaped but does not exist, create new account for now
                if account_audit.type_id != ACCOUNT_AUDIT_TYPE_NEW:
                    account.is_reaped = True
                    account.count_reaped = 1
                account.shard_num = 0

                print('NewAccountEventProcessor start add  account {} =='.format(account))

                account.save(db_session)
                # db_session.commit()

            account_audit.save(db_session)

    def accumulation_revert(self, db_session):
        for item in AccountAudit.query(db_session).filter_by(block_id=self.block.id):
            db_session.delete(item)


class ReapedAccount(EventProcessor):
    module_id = 'balances'
    event_id = 'ReapedAccount'

    def accumulation_hook(self, db_session):
        # Check event requirements
        if len(self.event.attributes) == 1 and \
                self.event.attributes[0]['type'] == 'AccountId':

            account_id = self.event.attributes[0]['value'].replace('0x', '')

            self.block._accounts_reaped.append(account_id)

            account_audit = AccountAudit(
                account_id=account_id,
                block_id=self.event.block_id,
                extrinsic_idx=self.event.extrinsic_idx,
                event_idx=self.event.event_idx,
                type_id=ACCOUNT_AUDIT_TYPE_REAPED
            )

            account_audit.save(db_session)

            # Insert account index audit record

            new_account_index_audit = AccountIndexAudit(
                account_index_id=None,
                account_id=account_id,
                block_id=self.event.block_id,
                extrinsic_idx=self.event.extrinsic_idx,
                event_idx=self.event.event_idx,
                type_id=ACCOUNT_INDEX_AUDIT_TYPE_REAPED
            )

            account_total = Account.query(db_session).filter_by(
                id=account_audit.account_id).count()
            print('ReapedAccount get account_total {} =='.format(account_total))

            if account_total <= 0:
                account = Account(
                    id=account_audit.account_id,
                    address=ss58_encode(account_audit.account_id, SUBSTRATE_ADDRESS_TYPE),
                    created_at_block=account_audit.block_id,
                    updated_at_block=account_audit.block_id,
                    balance=0
                )
                if account_audit.type_id == ACCOUNT_AUDIT_TYPE_REAPED:
                    account.count_reaped += 1
                    account.is_reaped = True

                elif account_audit.type_id == ACCOUNT_AUDIT_TYPE_NEW:
                    account.is_reaped = False

                account.updated_at_block = account_audit.block_id
                # If reaped but does not exist, create new account for now
                if account_audit.type_id != ACCOUNT_AUDIT_TYPE_NEW:
                    account.is_reaped = True
                    account.count_reaped = 1
                account.shard_num = 0

                print('ReapedAccount start add  account {} =='.format(account))

                account.save(db_session)
                # db_session.commit()

            new_account_index_audit.save(db_session)

    def accumulation_revert(self, db_session):

        for item in AccountIndexAudit.query(db_session).filter_by(block_id=self.block.id):
            db_session.delete(item)

        for item in AccountAudit.query(db_session).filter_by(block_id=self.block.id):
            db_session.delete(item)


class NewAccountIndexEventProcessor(EventProcessor):
    module_id = 'indices'
    event_id = 'NewAccountIndex'

    def accumulation_hook(self, db_session):
        account_id = self.event.attributes[0]['value'].replace('0x', '')
        id = self.event.attributes[1]['value']

        account_index_audit = AccountIndexAudit(
            account_index_id=id,
            account_id=account_id,
            block_id=self.event.block_id,
            extrinsic_idx=self.event.extrinsic_idx,
            event_idx=self.event.event_idx,
            type_id=ACCOUNT_INDEX_AUDIT_TYPE_NEW
        )

        account_index_audit.save(db_session)

    def accumulation_revert(self, db_session):
        for item in AccountIndexAudit.query(db_session).filter_by(block_id=self.block.id):
            db_session.delete(item)
