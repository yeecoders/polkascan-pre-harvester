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
#  converters.py
import math
import random
from _blake2 import blake2b

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError

from app.models.harvester import Status
from app.processors import Log
from app.type_registry import load_type_registry
from scalecodec import U32
from scalecodec.base import ScaleBytes, ScaleDecoder, RuntimeConfiguration
from scalecodec.exceptions import RemainingScaleBytesNotEmptyException
from scalecodec.metadata import MetadataDecoder
from scalecodec.block import ExtrinsicsDecoder, EventsDecoder, ExtrinsicsBlock61181Decoder

from app.processors.base import BaseService, ProcessorRegistry
from substrateinterface import SubstrateInterface, SubstrateRequestException

from app.settings import DEBUG, SUBSTRATE_RPC_URL, ACCOUNT_AUDIT_TYPE_NEW, NUM, ACCOUNT_INDEX_AUDIT_TYPE_NEW, \
    SUBSTRATE_MOCK_EXTRINSICS, FINALIZATION_BY_BLOCK_CONFIRMATIONS
from app.models.data import Extrinsic, Block, Event, Runtime, RuntimeModule, RuntimeCall, RuntimeCallParam, \
    RuntimeEvent, RuntimeEventAttribute, RuntimeType, RuntimeStorage, BlockTotal, RuntimeConstant, AccountAudit, \
    AccountIndexAudit, ReorgBlock, ReorgExtrinsic, ReorgEvent, ReorgLog
from app.processors.block import *
from app.utils import bech32
import json


class HarvesterCouldNotAddBlock(Exception):
    pass


class BlockAlreadyAdded(Exception):
    pass


class BlockIntegrityError(Exception):
    pass


class HarvesterNotshardParamsError(Exception):
    pass


class PolkascanHarvesterService(BaseService):

    def __init__(self, db_session, type_registry='default'):
        self.db_session = db_session
        RuntimeConfiguration().update_type_registry(load_type_registry('default'))
        if type_registry != 'default':
            RuntimeConfiguration().update_type_registry(load_type_registry(type_registry))
        self.metadata_store = {}

    def process_shard_genesis(self, block, substrate_url=None):
        substrate = SubstrateInterface(substrate_url)
        print('start process_shard_genesis  {} =='.format(substrate_url))

        # Set block time of parent block
        child_block = Block.query(self.db_session).filter_by(parent_hash=block.hash).first()

        if child_block.datetime is not None:
            {block.set_datetime(child_block.datetime)}

        # Retrieve genesis accounts
        storage_call = RuntimeStorage.query(self.db_session).filter_by(
            module_id='indices',
            name='NextEnumSet',
            spec_version=block.spec_version_id
        ).first()

        if storage_call:
            genesis_account_page_count = substrate.get_storage(
                block_hash=block.hash,
                module="Indices",
                function="NextEnumSet",
                return_scale_type=storage_call.get_return_type(),
                hasher=storage_call.type_hasher
            ) or 0

            # Get Accounts on EnumSet
            storage_call = RuntimeStorage.query(self.db_session).filter_by(
                module_id='indices',
                name='EnumSet',
                spec_version=block.spec_version_id
            ).first()

            if storage_call:

                block.count_accounts_new = 0
                block.count_accounts = 0

                for enum_set_nr in range(0, genesis_account_page_count + 1):

                    account_index_u32 = U32()
                    account_index_u32.encode(enum_set_nr)

                    genesis_accounts = substrate.get_storage(
                        block_hash=block.hash,
                        module="Indices",
                        function="EnumSet",
                        params=account_index_u32.data.data.hex(),
                        return_scale_type=storage_call.get_return_type(),
                        hasher=storage_call.type_hasher
                    )

                    if genesis_accounts:
                        print('storage_hash start  genesis_accounts {} =='.format(genesis_accounts))
                        block.count_accounts_new += len(genesis_accounts)
                        block.count_accounts += len(genesis_accounts)

                        for idx, account_id in enumerate(genesis_accounts):
                            account_audit = AccountAudit(
                                account_id=account_id.replace('0x', ''),
                                block_id=block.id,
                                extrinsic_idx=None,
                                event_idx=None,
                                type_id=ACCOUNT_AUDIT_TYPE_NEW
                            )

                            account_total = Account.query(self.db_session).filter_by(
                                id=account_audit.account_id).count()
                            print('process_shard_genesis get account_total {} =='.format(account_total))

                            if account_total <= 0:
                                account = Account(
                                    id=account_audit.account_id,
                                    address=bech32.encode(HRP, bytes().fromhex(account_audit.account_id)),
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
                                account.shard_num = NUM[substrate_url]

                                print('start add  account {} =='.format(account))
                                if Account.query(self.db_session).filter_by(id=account_audit.account_id).count() == 0:
                                    account.save(self.db_session)
                                self.db_session.commit()
                            if AccountAudit.query(self.db_session).filter_by(
                                    account_id=account_audit.account_id).count() == 0:
                                account_audit.save(self.db_session)

                            account_index_id = enum_set_nr * 64 + idx

                            account_index_audit = AccountIndexAudit(
                                account_index_id=account_index_id,
                                account_id=account_id.replace('0x', ''),
                                block_id=block.id,
                                extrinsic_idx=None,
                                event_idx=None,
                                type_id=ACCOUNT_INDEX_AUDIT_TYPE_NEW
                            )
                            if AccountIndexAudit.query(self.db_session).filter_by(
                                    account_id=account_index_audit.account_id).count() == 0:
                                account_index_audit.save(self.db_session)

        block.save(self.db_session)

    def process_genesis(self, block, substrate_url=None):
        substrate = SubstrateInterface(substrate_url)

        # Set block time of parent block
        child_block = Block.query(self.db_session).filter_by(parent_hash=block.hash).first()

        if child_block.datetime is not None:
            {block.set_datetime(child_block.datetime)}

        # Retrieve genesis accounts
        storage_call = RuntimeStorage.query(self.db_session).filter_by(
            module_id='indices',
            name='NextEnumSet',
            spec_version=block.spec_version_id
        ).first()

        if storage_call:
            genesis_account_page_count = substrate.get_storage(
                block_hash=block.hash,
                module="Indices",
                function="NextEnumSet",
                return_scale_type=storage_call.get_return_type(),
                hasher=storage_call.type_hasher
            ) or 0

            # Get Accounts on EnumSet
            storage_call = RuntimeStorage.query(self.db_session).filter_by(
                module_id='indices',
                name='EnumSet',
                spec_version=block.spec_version_id
            ).first()

            if storage_call:

                block.count_accounts_new = 0
                block.count_accounts = 0

                for enum_set_nr in range(0, genesis_account_page_count + 1):

                    account_index_u32 = U32()
                    account_index_u32.encode(enum_set_nr)

                    genesis_accounts = substrate.get_storage(
                        block_hash=block.hash,
                        module="Indices",
                        function="EnumSet",
                        params=account_index_u32.data.data.hex(),
                        return_scale_type=storage_call.get_return_type(),
                        hasher=storage_call.type_hasher
                    )

                    if genesis_accounts:
                        block.count_accounts_new += len(genesis_accounts)
                        block.count_accounts += len(genesis_accounts)

                        for idx, account_id in enumerate(genesis_accounts):
                            account_audit = AccountAudit(
                                account_id=account_id.replace('0x', ''),
                                block_id=block.id,
                                extrinsic_idx=None,
                                event_idx=None,
                                type_id=ACCOUNT_AUDIT_TYPE_NEW
                            )

                            account_audit.save(self.db_session)

                            account_index_id = enum_set_nr * 64 + idx

                            account_index_audit = AccountIndexAudit(
                                account_index_id=account_index_id,
                                account_id=account_id.replace('0x', ''),
                                block_id=block.id,
                                extrinsic_idx=None,
                                event_idx=None,
                                type_id=ACCOUNT_INDEX_AUDIT_TYPE_NEW
                            )

                            account_index_audit.save(self.db_session)

        block.save(self.db_session)

    def process_metadata_type(self, type_string, spec_version):

        runtime_type = RuntimeType.query(self.db_session).filter_by(type_string=type_string,
                                                                    spec_version=spec_version).first()

        if not runtime_type:

            # Get current Runtime configuration
            try:
                # TODO move logic to RuntimeConfiguration.get_decoder_class
                # TODO FIX ScaleBytes('0x00') does not process Option<*> properly
                decoder_obj = ScaleDecoder.get_decoder_class(type_string, ScaleBytes('0x00'))

                if decoder_obj.sub_type:

                    # Also process sub type
                    if ',' in decoder_obj.sub_type and decoder_obj.sub_type[-1:] not in ['>', ')']:
                        for sub_type in decoder_obj.sub_type.split(','):
                            self.process_metadata_type(sub_type.strip(), spec_version)
                    else:
                        self.process_metadata_type(decoder_obj.sub_type, spec_version)

                decoder_class_name = decoder_obj.__class__.__name__

            except NotImplementedError:
                decoder_class_name = '[not implemented]'

            runtime_type = RuntimeType(
                spec_version=spec_version,
                type_string=type_string,
                decoder_class=decoder_class_name,
            )

            runtime_type.save(self.db_session)

    def process_metadata(self, runtime_version_data, block_hash, substrate_url=None):
        if substrate_url is None:
            raise HarvesterNotshardParamsError('params shard is missing.. stopping harvester ')
        spec_version = runtime_version_data.get('specVersion', 0)

        # Check if metadata already in store
        if spec_version not in self.metadata_store:
            print('Metadata: CACHE MISS', spec_version)

            runtime = Runtime.query(self.db_session).get(spec_version)

            if runtime:

                metadata_decoder = MetadataDecoder(ScaleBytes(runtime.json_metadata))
                metadata_decoder.decode()

                self.metadata_store[spec_version] = metadata_decoder

            else:
                self.db_session.begin(subtransactions=True)
                try:

                    # ==== Get block Metadata from Substrate ==================
                    substrate = SubstrateInterface(substrate_url)
                    metadata_decoder = substrate.get_block_metadata(block_hash)

                    # Store metadata in database
                    runtime = Runtime(
                        id=spec_version,
                        impl_name=runtime_version_data["implName"],
                        impl_version=runtime_version_data["implVersion"],
                        spec_name=runtime_version_data["specName"],
                        spec_version=spec_version,
                        json_metadata=str(metadata_decoder.data),
                        json_metadata_decoded=metadata_decoder.value,
                        apis=runtime_version_data["apis"],
                        authoring_version=runtime_version_data["authoringVersion"],
                        count_call_functions=0,
                        count_events=0,
                        count_modules=len(metadata_decoder.metadata.modules),
                        count_storage_functions=0
                    )

                    runtime.save(self.db_session)

                    print('store version to db', metadata_decoder.version)

                    if not metadata_decoder.version:
                        # Legacy V0 fallback
                        for module in metadata_decoder.metadata.modules:
                            runtime_module = RuntimeModule(
                                spec_version=spec_version,
                                module_id=module.get_identifier(),
                                prefix=module.prefix,
                                name=module.get_identifier(),
                                count_call_functions=len(module.functions or []),
                                count_storage_functions=len(module.storage or []),
                                count_events=0
                            )
                            runtime_module.save(self.db_session)

                            if len(module.functions or []) > 0:
                                for idx, call in enumerate(module.functions):
                                    runtime_call = RuntimeCall(
                                        spec_version=spec_version,
                                        module_id=module.get_identifier(),
                                        call_id=call.get_identifier(),
                                        index=idx,
                                        name=call.name,
                                        lookup=call.lookup,
                                        documentation='\n'.join(call.docs),
                                        count_params=len(call.args)
                                    )
                                    runtime_call.save(self.db_session)

                                    for arg in call.args:
                                        runtime_call_param = RuntimeCallParam(
                                            runtime_call_id=runtime_call.id,
                                            name=arg.name,
                                            type=arg.type
                                        )
                                        runtime_call_param.save(self.db_session)

                                        # Check if type already registered in database
                                        self.process_metadata_type(arg.type, spec_version)

                        for event_module in metadata_decoder.metadata.events_modules:
                            for event_index, event in enumerate(event_module.events):
                                runtime_event = RuntimeEvent(
                                    spec_version=spec_version,
                                    module_id=event_module.name,
                                    event_id=event.name,
                                    index=event_index,
                                    name=event.name,
                                    lookup=event.lookup,
                                    documentation='\n'.join(event.docs),
                                    count_attributes=len(event.args)
                                )
                                runtime_event.save(self.db_session)

                                runtime_module.count_events += 1

                                for arg_index, arg in enumerate(event.args):
                                    runtime_event_attr = RuntimeEventAttribute(
                                        runtime_event_id=runtime_event.id,
                                        index=arg_index,
                                        type=arg
                                    )
                                    runtime_event_attr.save(self.db_session)

                        runtime_module.save(self.db_session)

                    else:
                        for module in metadata_decoder.metadata.modules:

                            # Check if module exists
                            if RuntimeModule.query(self.db_session).filter_by(
                                    spec_version=spec_version,
                                    module_id=module.get_identifier()
                            ).count() == 0:
                                module_id = module.get_identifier()
                            else:
                                module_id = '{}_1'.format(module.get_identifier())

                            # Storage backwards compt check
                            if module.storage and isinstance(module.storage, list):
                                storage_functions = module.storage
                            elif module.storage and isinstance(getattr(module.storage, 'value'), dict):
                                storage_functions = module.storage.items
                            else:
                                storage_functions = []

                            runtime_module = RuntimeModule(
                                spec_version=spec_version,
                                module_id=module_id,
                                prefix=module.prefix,
                                name=module.name,
                                count_call_functions=len(module.calls or []),
                                count_storage_functions=len(storage_functions),
                                count_events=len(module.events or [])
                            )
                            runtime_module.save(self.db_session)

                            # Update totals in runtime
                            runtime.count_call_functions += runtime_module.count_call_functions
                            runtime.count_events += runtime_module.count_events
                            runtime.count_storage_functions += runtime_module.count_storage_functions

                            if len(module.calls or []) > 0:
                                for idx, call in enumerate(module.calls):
                                    runtime_call = RuntimeCall(
                                        spec_version=spec_version,
                                        module_id=module_id,
                                        call_id=call.get_identifier(),
                                        index=idx,
                                        name=call.name,
                                        lookup=call.lookup,
                                        documentation='\n'.join(call.docs),
                                        count_params=len(call.args)
                                    )
                                    runtime_call.save(self.db_session)

                                    for arg in call.args:
                                        runtime_call_param = RuntimeCallParam(
                                            runtime_call_id=runtime_call.id,
                                            name=arg.name,
                                            type=arg.type
                                        )
                                        runtime_call_param.save(self.db_session)

                                        # Check if type already registered in database
                                        self.process_metadata_type(arg.type, spec_version)

                            if len(module.events or []) > 0:
                                for event_index, event in enumerate(module.events):
                                    runtime_event = RuntimeEvent(
                                        spec_version=spec_version,
                                        module_id=module_id,
                                        event_id=event.name,
                                        index=event_index,
                                        name=event.name,
                                        lookup=event.lookup,
                                        documentation='\n'.join(event.docs),
                                        count_attributes=len(event.args)
                                    )
                                    runtime_event.save(self.db_session)

                                    for arg_index, arg in enumerate(event.args):
                                        runtime_event_attr = RuntimeEventAttribute(
                                            runtime_event_id=runtime_event.id,
                                            index=arg_index,
                                            type=arg
                                        )
                                        runtime_event_attr.save(self.db_session)

                            if len(storage_functions) > 0:
                                for idx, storage in enumerate(storage_functions):

                                    # Determine type
                                    type_hasher = None
                                    type_key1 = None
                                    type_key2 = None
                                    type_value = None
                                    type_is_linked = None
                                    type_key2hasher = None

                                    if storage.type.get('PlainType'):
                                        type_value = storage.type.get('PlainType')

                                    elif storage.type.get('MapType'):
                                        type_hasher = storage.type['MapType'].get('hasher')
                                        type_key1 = storage.type['MapType'].get('key')
                                        type_value = storage.type['MapType'].get('value')
                                        type_is_linked = storage.type['MapType'].get('isLinked', False)

                                    elif storage.type.get('DoubleMapType'):
                                        type_hasher = storage.type['DoubleMapType'].get('hasher')
                                        type_key1 = storage.type['DoubleMapType'].get('key1')
                                        type_key2 = storage.type['DoubleMapType'].get('key2')
                                        type_value = storage.type['DoubleMapType'].get('value')
                                        type_key2hasher = storage.type['DoubleMapType'].get('key2Hasher')

                                    runtime_storage = RuntimeStorage(
                                        spec_version=spec_version,
                                        module_id=module_id,
                                        index=idx,
                                        name=storage.name,
                                        lookup=None,
                                        default=storage.fallback,
                                        modifier=storage.modifier,
                                        type_hasher=type_hasher,
                                        type_key1=type_key1,
                                        type_key2=type_key2,
                                        type_value=type_value,
                                        type_is_linked=type_is_linked,
                                        type_key2hasher=type_key2hasher,
                                        documentation='\n'.join(storage.docs)
                                    )
                                    runtime_storage.save(self.db_session)

                                    # Check if types already registered in database

                                    self.process_metadata_type(type_value, spec_version)

                                    if type_key1:
                                        self.process_metadata_type(type_key1, spec_version)

                                    if type_key2:
                                        self.process_metadata_type(type_key2, spec_version)

                            if len(module.constants or []) > 0:
                                for idx, constant in enumerate(module.constants):

                                    # Decode value
                                    try:
                                        value_obj = ScaleDecoder.get_decoder_class(
                                            constant.type,
                                            ScaleBytes(constant.constant_value)
                                        )
                                        value_obj.decode()
                                        value = value_obj.serialize()
                                    except ValueError:
                                        value = constant.constant_value
                                    except RemainingScaleBytesNotEmptyException:
                                        value = constant.constant_value
                                    except NotImplementedError:
                                        value = constant.constant_value

                                    runtime_constant = RuntimeConstant(
                                        spec_version=spec_version,
                                        module_id=module_id,
                                        index=idx,
                                        name=constant.name,
                                        type=constant.type,
                                        value=value,
                                        documentation='\n'.join(constant.docs)
                                    )
                                    runtime_constant.save(self.db_session)

                                    # Check if types already registered in database
                                    self.process_metadata_type(constant.type, spec_version)

                        runtime.save(self.db_session)

                    self.db_session.commit()

                    # Put in local store
                    self.metadata_store[spec_version] = metadata_decoder
                except SQLAlchemyError as e:
                    self.db_session.rollback()

    def add_block(self, block_hash, substrate_url=None):
        #print('start add_block substrate_url {} =='.format(substrate_url))
        print('start add_block block_hasubstratesh {} =='.format(block_hash))
        if substrate_url is None:
            raise HarvesterNotshardParamsError('params shard is missing.. stopping harvester ')

        shard_num = NUM[substrate_url]
        #print('start shard_num shard_num {} =='.format(shard_num))
        # Check if block is already process

        bq = Block.query(self.db_session).filter_by(hash=block_hash).first()

        if bq:
            #print('bq: {} =='.format(bq.bid))
            #print('start add_block BlockAlreadyAdded {} =='.format(block_hash))
            return bq
        # Extract data from json_block

        substrate = SubstrateInterface(substrate_url)

        if SUBSTRATE_MOCK_EXTRINSICS:
            substrate.mock_extrinsics = SUBSTRATE_MOCK_EXTRINSICS

        json_block = substrate.get_chain_block(block_hash)
        #print('start add_block json_block {} =='.format(json_block))

        parent_hash = json_block['block']['header'].pop('parentHash')
        block_id = json_block['block']['header'].pop('number')
        extrinsics_root = json_block['block']['header'].pop('extrinsicsRoot')
        state_root = json_block['block']['header'].pop('stateRoot')
        digest_logs = json_block['block']['header'].get('digest', {}).pop('logs', None)
        # decode logs
        mpmr = ''
        if len(digest_logs) != 0:
            for dg in digest_logs:
                if dg[0:4] == '0x04':
                    mpmr = '0x' + self.decode_log_pow(dg)
                elif dg[0:8] == '0x001802':
                    # 0x0018020002000400
                    shard_num = dg[10:12]

        # Convert block number to numeric
        if not block_id.isnumeric():
            block_id = int(block_id, 16)

        # ==== Get block runtime from Substrate ==================
        json_runtime_version = substrate.get_block_runtime_version()

        # Get spec version
        spec_version = json_runtime_version.get('specVersion', 0)

        if Runtime.query(self.db_session).filter_by(impl_name='yee-rs').count() <= 0:
            #print('start add_block process_metadata {} =='.format("process_metadata"))
            self.process_metadata(json_runtime_version, block_hash, substrate_url)

        # ==== Get parent block runtime ===================

        if block_id > 0:
            json_parent_runtime_version = substrate.get_block_runtime_version()

            parent_spec_version = json_parent_runtime_version.get('specVersion', 0)

            self.process_metadata(json_parent_runtime_version, parent_hash, substrate_url)
        else:
            parent_spec_version = spec_version

        # ==== Set initial block properties =====================

        block = Block(
            bid=block_id,
            parent_id=block_id - 1,
            hash=block_hash,
            parent_hash=parent_hash,
            state_root=state_root,
            extrinsics_root=extrinsics_root,
            count_extrinsics=0,
            count_events=0,
            count_accounts_new=0,
            count_accounts_reaped=0,
            count_accounts=0,
            count_events_extrinsic=0,
            count_events_finalization=0,
            count_events_module=0,
            count_events_system=0,
            count_extrinsics_error=0,
            count_extrinsics_signed=0,
            count_extrinsics_signedby_address=0,
            count_extrinsics_signedby_index=0,
            count_extrinsics_success=0,
            count_extrinsics_unsigned=0,
            count_sessions_new=0,
            count_contracts_new=0,
            count_log=0,
            range10000=math.floor(block_id / 10000),
            range100000=math.floor(block_id / 100000),
            range1000000=math.floor(block_id / 1000000),
            spec_version_id=spec_version,
            logs=digest_logs,
            shard_num=shard_num,
            mpmr=mpmr

        )

        # Set temp helper variables
        block._accounts_new = []
        block._accounts_reaped = []

        # ==== Get block events from Substrate ==================
        extrinsic_success_idx = {}
        events = []
        block_reward = 0
        coinbase = HRP
        reward_block_number = 0
        fee_reward = 0

        try:
            events_decoder = substrate.get_block_events(block_hash, self.metadata_store[parent_spec_version])

            if events_decoder != None:

                event_idx = 0

                for event in events_decoder.elements:

                    event.value['module_id'] = event.value['module_id'].lower()

                    model = Event(
                        block_id=block_id,
                        event_idx=event_idx,
                        phase=event.value['phase'],
                        extrinsic_idx=event.value['extrinsic_idx'],
                        type=event.value['type'],
                        spec_version_id=parent_spec_version,
                        module_id=event.value['module_id'],
                        event_id=event.value['event_id'],
                        system=int(event.value['module_id'] == 'system'),
                        module=int(event.value['module_id'] != 'system'),
                        attributes=event.value['params'],
                        codec_error=False,
                        shard_num=shard_num
                    )

                    # Process event
                    if event.value['event_id'] == 'Reward':
                        data1 = event.value['params'][0]
                        json_str = json.dumps(data1)
                        data2 = json.loads(json_str)
                        block_reward = data2['value']['block_reward']
                        coinbase = data2['value']['coinbase']
                        if coinbase != HRP:
                            coinbase = bech32.encode(HRP, bytes().fromhex(data2['value']['coinbase'][2:]))

                        reward_block_number = data2['value']['block_number']
                        fee_reward = data2['value']['fee_reward']

                        #print(' event params ---{}'.format(data1))

                    if event.value['phase'] == 0:
                        block.count_events_extrinsic += 1
                    elif event.value['phase'] == 1:
                        block.count_events_finalization += 1

                    if event.value['module_id'] == 'system':

                        block.count_events_system += 1

                        # Store result of extrinsic
                        if event.value['event_id'] == 'ExtrinsicSuccess':
                            extrinsic_success_idx[event.value['extrinsic_idx']] = True
                            block.count_extrinsics_success += 1

                        if event.value['event_id'] == 'ExtrinsicFailed':
                            extrinsic_success_idx[event.value['extrinsic_idx']] = False
                            block.count_extrinsics_error += 1
                    else:

                        block.count_events_module += 1

                    model.save(self.db_session)

                    events.append(model)

                    event_idx += 1

                block.count_events = len(events_decoder.elements)

        except SubstrateRequestException:
            block.count_events = 0

        # === Extract extrinsics from block ====

        extrinsics_data = json_block['block'].pop('extrinsics')
        num = 10000
        origin_hash = ''

        block.count_extrinsics = len(extrinsics_data)
        #print('start add_block block.count_extrinsics {} =='.format(block.count_extrinsics))
        if block.count_extrinsics != 0:

            extrinsic_idx = 0

            extrinsics = []

            for extrinsic in extrinsics_data:

                # Save to data table
                if block_hash == '0x911a0bf66d5494b6b24f612b3cc14841134c6b73ab9ce02f7e012973070e5661':
                    # TODO TEMP fix for exception in Alexander network, remove when network is obsolete
                    extrinsics_decoder = ExtrinsicsBlock61181Decoder(
                        data=ScaleBytes(extrinsic),
                        metadata=self.metadata_store[parent_spec_version]
                    )
                else:
                    extrinsics_decoder = ExtrinsicsDecoder(
                        data=ScaleBytes(extrinsic),
                        metadata=self.metadata_store[parent_spec_version]
                    )

                extrinsic_data = extrinsics_decoder.decode()

                # Lookup result of extrinsic

                extrinsic_success = extrinsic_success_idx.get(extrinsic_idx, False)
                if extrinsic_data.get('call_module') == 'sharding':
                    num = int(extrinsic_data.get('params')[0]['value']['num'])
                    #print('start add_block extrinsic_data decoder-get num  {} ={}='.format(num, substrate_url))

                if extrinsic_data.get('call_module_function') == 'relay_transfer':
                    origin_hash = blake2b(bytearray.fromhex(extrinsic_data.get('params')[0]['value']),
                                          digest_size=32).digest().hex()
                    #print('start add_block extrinsic_data decoder-get origin_hash  {} ={}='.format(origin_hash,substrate_url))
                if extrinsic_data.get('call_module_function') == 'transfer' and extrinsic_data.get('call_code') =='0900':
                    print('extrinsic_data.call_code=', extrinsic_data.get('call_code'))
                    print('extrinsic_data.getparams=', extrinsic_data.get('params')[1]['value'])
                    origin_hash = blake2b(bytearray.fromhex(extrinsic_data.get('params')[1]['value']),
                                          digest_size=32).digest().hex()

                model = Extrinsic(
                    block_id=block_id,
                    extrinsic_idx=extrinsic_idx,
                    extrinsic_hash=extrinsics_decoder.extrinsic_hash,
                    extrinsic_length=extrinsic_data.get('extrinsic_length'),
                    extrinsic_version=extrinsic_data.get('version_info'),
                    signed=extrinsics_decoder.contains_transaction,
                    unsigned=not extrinsics_decoder.contains_transaction,
                    signedby_address=bool(extrinsics_decoder.contains_transaction and extrinsic_data.get('account_id')),
                    signedby_index=bool(
                        extrinsics_decoder.contains_transaction and extrinsic_data.get('account_index')),
                    address_length=extrinsic_data.get('account_length'),
                    address=extrinsic_data.get('account_id'),
                    account_index=extrinsic_data.get('account_index'),
                    account_idx=extrinsic_data.get('account_idx'),
                    signature=extrinsic_data.get('signature'),
                    nonce=extrinsic_data.get('nonce'),
                    era=extrinsic_data.get('era'),
                    call=extrinsic_data.get('call_code'),
                    module_id=extrinsic_data.get('call_module'),
                    call_id=extrinsic_data.get('call_module_function'),
                    params=extrinsic_data.get('params'),
                    spec_version_id=parent_spec_version,
                    success=int(extrinsic_success),
                    error=int(not extrinsic_success),
                    codec_error=False,
                    shard_num=shard_num,
                    datetime=block.datetime,
                    origin_hash=origin_hash
                )
                model.save(self.db_session)

                extrinsics.append(model)

                extrinsic_idx += 1

                # Process extrinsic
                if extrinsics_decoder.contains_transaction:
                    block.count_extrinsics_signed += 1

                    if model.signedby_address:
                        block.count_extrinsics_signedby_address += 1
                    if model.signedby_index:
                        block.count_extrinsics_signedby_index += 1

                else:
                    block.count_extrinsics_unsigned += 1

                # Process extrinsic processors
                for processor_class in ProcessorRegistry().get_extrinsic_processors(model.module_id, model.call_id):
                    extrinsic_processor = processor_class(block, model)
                    extrinsic_processor.accumulation_hook(self.db_session)

            # Process event processors
            for event in events:
                extrinsic = None
                if event.extrinsic_idx is not None:
                    try:
                        extrinsic = extrinsics[event.extrinsic_idx]
                    except IndexError:
                        extrinsic = None

                for processor_class in ProcessorRegistry().get_event_processors(event.module_id, event.event_id):
                    event_processor = processor_class(block, event, extrinsic,
                                                      metadata=self.metadata_store.get(block.spec_version_id))
                    event_processor.accumulation_hook(self.db_session)

        # Process block processors
        #print('start add_block Process block processors {} =='.format("Process block processors"))

        for processor_class in ProcessorRegistry().get_block_processors():
            block_processor = processor_class(block)
            block_processor.accumulation_hook(self.db_session)

        # Debug info
        #print('start add_block Debuginfo {} =='.format("Debuginfo"))

        if DEBUG:
            block.debug_info = json_block

        # ==== Save data block ==================================
        block.validators = ''
        block.coinbase = coinbase
        block.block_reward = block_reward
        block.reward_block_number = reward_block_number
        block.fee_reward = fee_reward

        block.save(self.db_session)
        #print('start add_block return {} =='.format("return"))

        return block

    def remove_block(self, block_hash):
        # Retrieve block
        block = Block.query(self.db_session).filter_by(hash=block_hash).first()

        # Revert event processors
        for event in Event.query(self.db_session).filter_by(block_id=block.id):
            for processor_class in ProcessorRegistry().get_event_processors(event.module_id, event.event_id):
                event_processor = processor_class(block, event, None)
                event_processor.accumulation_revert(self.db_session)

        # Revert extrinsic processors
        for extrinsic in Extrinsic.query(self.db_session).filter_by(block_id=block.id):
            for processor_class in ProcessorRegistry().get_extrinsic_processors(extrinsic.module_id, extrinsic.call_id):
                extrinsic_processor = processor_class(block, extrinsic)
                extrinsic_processor.accumulation_revert(self.db_session)

        # Revert block processors
        for processor_class in ProcessorRegistry().get_block_processors():
            block_processor = processor_class(block)
            block_processor.accumulation_revert(self.db_session)

        # Delete events
        for item in Event.query(self.db_session).filter_by(block_id=block.id):
            self.db_session.delete(item)
        # Delete extrinsics
        for item in Extrinsic.query(self.db_session).filter_by(block_id=block.id):
            self.db_session.delete(item)

        # Delete block
        self.db_session.delete(block)

    def sequence_block(self, block, parent_block_data=None, parent_sequenced_block_data=None):

        sequenced_block = BlockTotal(
            id=block.id
        )

        if block:
            # Process block processors
            for processor_class in ProcessorRegistry().get_block_processors():
                block_processor = processor_class(block, sequenced_block)
                block_processor.sequencing_hook(
                    self.db_session,
                    parent_block_data,
                    parent_sequenced_block_data
                )

            extrinsics = Extrinsic.query(self.db_session).filter_by(block_id=block.id)

            for extrinsic in extrinsics:
                # Process extrinsic processors
                for processor_class in ProcessorRegistry().get_extrinsic_processors(extrinsic.module_id,
                                                                                    extrinsic.call_id):
                    extrinsic_processor = processor_class(block, extrinsic)
                    extrinsic_processor.sequencing_hook(
                        self.db_session,
                        parent_block_data,
                        parent_sequenced_block_data
                    )

            events = Event.query(self.db_session).filter_by(block_id=block.id).order_by('event_idx')

            # Process event processors
            for event in events:
                extrinsic = None
                if event.extrinsic_idx is not None:
                    try:
                        extrinsic = extrinsics[event.extrinsic_idx]
                    except IndexError:
                        extrinsic = None

                for processor_class in ProcessorRegistry().get_event_processors(event.module_id, event.event_id):
                    event_processor = processor_class(block, event, extrinsic)
                    event_processor.sequencing_hook(
                        self.db_session,
                        parent_block_data,
                        parent_sequenced_block_data
                    )

        sequenced_block.save(self.db_session)

        return sequenced_block

    def integrity_checks(self):

        # 1. Check finalized head
        substrate = SubstrateInterface(SUBSTRATE_RPC_URL)

        if FINALIZATION_BY_BLOCK_CONFIRMATIONS > 0:
            finalized_block_hash = substrate.get_chain_head()
            finalized_block_number = max(
                substrate.get_block_number(finalized_block_hash) - FINALIZATION_BY_BLOCK_CONFIRMATIONS, 0
            )
        else:
            finalized_block_hash = substrate.get_chain_head()
            finalized_block_number = substrate.get_block_number(finalized_block_hash)

        # 2. Check integrity head
        integrity_head = Status.get_status(self.db_session, 'INTEGRITY_HEAD')

        if not integrity_head.value:
            # Only continue if block #1 exists
            # if Block.query(self.db_session).filter_by(id=1).count() == 0:
            #     raise BlockIntegrityError('BlockIntegrityError-Chain not at genesis')

            integrity_head.value = 0
        else:
            integrity_head.value = int(integrity_head.value)

        start_block_id = max(integrity_head.value - 1, 0)
        end_block_id = finalized_block_number
        chunk_size = 1000
        parent_block = None
        print('== outer Start integrity checks from {} to {} =='.format(start_block_id, end_block_id))

        if start_block_id < end_block_id:
            # Continue integrity check

            print('== Start integrity checks from {} to {} =='.format(start_block_id, end_block_id))

            for block_nr in range(start_block_id, end_block_id, chunk_size):
                # TODO replace limit with filter_by block range
                block_range = Block.query(self.db_session).order_by('id')[block_nr:block_nr + chunk_size]
                for block in block_range:
                    if parent_block:
                        if block.id != parent_block.id + 1:

                            # Save integrity head if block hash of parent matches with hash in node
                            if parent_block.hash == substrate.get_block_hash(integrity_head.value):
                                integrity_head.save(self.db_session)
                                self.db_session.commit()

                            raise BlockIntegrityError(
                                'Block #{} is missing.. stopping check '.format(parent_block.id + 1))
                        elif block.parent_hash != parent_block.hash:

                            self.process_reorg_block(parent_block)
                            self.process_reorg_block(block)

                            self.remove_block(block.hash)
                            self.remove_block(parent_block.hash)
                            self.db_session.commit()

                            self.add_block(substrate.get_block_hash(block.id))
                            self.add_block(substrate.get_block_hash(parent_block.id))
                            self.db_session.commit()

                            integrity_head.value = parent_block.id - 1

                            # Save integrity head if block hash of parent matches with hash in node
                            # if parent_block.parent_hash == substrate.get_block_hash(integrity_head.value):
                            integrity_head.save(self.db_session)
                            self.db_session.commit()

                            raise BlockIntegrityError(
                                'Block #{} failed integrity checks, Re-adding #{}.. '.format(parent_block.id, block.id))
                        else:
                            integrity_head.value = block.id

                    parent_block = block

                    if block.id == end_block_id:
                        break

            if parent_block:
                if parent_block.hash == substrate.get_block_hash(int(integrity_head.value)):
                    integrity_head.save(self.db_session)
                    self.db_session.commit()

        return {'integrity_head': integrity_head.value}

    def start_sequencer(self, substrate_url=None):
        # integrity_status = self.integrity_checks()
        # self.db_session.commit()

        block_nr = None

        integrity_head = Status.get_status(self.db_session, 'INTEGRITY_HEAD')

        if not integrity_head.value:
            integrity_head.value = 0

        # 3. Check sequence head
        sequencer_head = self.db_session.query(func.max(BlockTotal.id)).one()[0]

        if sequencer_head is None:
            sequencer_head = -1

        # Start sequencing process

        sequencer_parent_block = BlockTotal.query(self.db_session).filter_by(id=sequencer_head).first()
        parent_block = Block.query(self.db_session).filter_by(id=sequencer_head).first()

        for block_nr in range(sequencer_head + 1, int(integrity_head.value) + 1):

            if block_nr == 0:
                # No block ever sequenced, check if chain is at genesis state
                assert (not sequencer_parent_block)

                block = Block.query(self.db_session).order_by('id').first()

                # if not block:
                #     self.db_session.commit()
                #     return {'error': 'not block Chain not at genesis'}
                #
                # if block.id == 1:
                #     # Add genesis block
                #     has_block = Block.query(self.db_session).filter_by(hash=block.parent_hash).first()
                #     if has_block is not None:
                #         block = self.add_block(block.parent_hash)
                #
                # if block.id != 0:
                #     self.db_session.commit()
                #     return {'error': 'block.id != 0--Chain not at genesis'}

                self.process_genesis(block)

                sequencer_parent_block_data = None
                parent_block_data = None
            else:
                block_id = sequencer_parent_block.id + 1

                assert (block_id == block_nr)

                block = Block.query(self.db_session).get(block_nr)

                sequencer_parent_block_data = sequencer_parent_block.asdict()
                parent_block_data = parent_block.asdict()

            sequenced_block = self.sequence_block(block, parent_block_data, sequencer_parent_block_data)
            self.db_session.commit()

            parent_block = block
            sequencer_parent_block = sequenced_block

        if block_nr:
            return {'result': 'Finished at #{}'.format(block_nr)}
        else:
            return {'result': 'Nothing to sequence'}

    def process_reorg_block(self, block):
        model = ReorgBlock(**block.asdict())
        model.save(self.db_session)

        for extrinsic in Extrinsic.query(self.db_session).filter_by(block_id=block.id):
            model = ReorgExtrinsic(block_hash=block.hash, **extrinsic.asdict())
            model.save(self.db_session)

        for event in Event.query(self.db_session).filter_by(block_id=block.id):
            model = ReorgEvent(block_hash=block.hash, **event.asdict())
            model.save(self.db_session)

        for log in Log.query(self.db_session).filter_by(block_id=block.id):
            model = ReorgLog(block_hash=block.hash, **log.asdict())
            model.save(self.db_session)

    def decode_log_pow(self, input):
        # if input[0:4] != '0x04':
        #     return
        input = input[2:]
        obj = ScaleDecoder.get_decoder_class('Compact<u32>', ScaleBytes(bytearray.fromhex(input[10:20])))
        obj.decode()
        veclen = obj.value
        if veclen <= 0b00111111:
            compactLen = 1
        elif veclen <= 0b0011111111111111:
            compactLen = 2

        elif veclen <= 0b00111111111111111111111111111111:
            compactLen = 4
        else:
            compactLen = 5

        len = 2 + 8 + compactLen * 2 + 64 + 64 + 16

        workProofType = ScaleDecoder.get_decoder_class('U16', ScaleBytes('0x' + str(input[len:len + 2])))
        workProofType.decode()

        workProofOffset = 2 + 8 + compactLen * 2 + 64 + 64 + 16 + 2

        if workProofType.value == 2:
           mlen = workProofOffset + 80
           merkle_root = input[mlen:mlen + 64]
           print(merkle_root)
        return merkle_root


import unittest
from scalecodec.base import ScaleDecoder, ScaleBytes, RemainingScaleBytesNotEmptyException
from scalecodec.block import ExtrinsicsDecoder, MetadataDecoder


class TestScaleTypes(unittest.TestCase):
    def test_log(self):
        # logs in header
        # "logs": [
        #     "0x00 1802 000000 0400",
        #      0x00 1802 000100 0400
        #      0x00 1802 000200 0400
        #     "0x00ed03030000000000000000001850907ff82b2d1fb8d7d07fe9cc0c6e33454e1e98abce6714d20567fbf33be078010000000000000050907ff82b2d1fb8d7d07fe9cc0c6e33454e1e98abce6714d20567fbf33be078010000000000000050907ff82b2d1fb8d7d07fe9cc0c6e33454e1e98abce6714d20567fbf33be078010000000000000050907ff82b2d1fb8d7d07fe9cc0c6e33454e1e98abce6714d20567fbf33be078010000000000000050907ff82b2d1fb8d7d07fe9cc0c6e33454e1e98abce6714d20567fbf33be078010000000000000050907ff82b2d1fb8d7d07fe9cc0c6e33454e1e98abce6714d20567fbf33be0780100000000000000",
        #     "0x002804008a0b000000000000",
        #     "0x0459656521750350907ff82b2d1fb8d7d07fe9cc0c6e33454e1e98abce6714d20567fbf33be078dee67cd3e950d9ecc58898fd0f94e94865f7e226e66e66b43683ed870c0100004e2989c06e01000002287965652d7377697463689f3314763de4e45e5c609767a4f860515a29503f50096bfa7ee3d81d769be64f2b15000000000000085afb1172f6a2a4611043764a56a6ce759f30fefb156293b4e26ba1a42b4f8ce278c86f19241fe16267983b35b4994d824acf5f8884aa98dabfbaed6ae41d28e185c09af929492a871e4fae32d9d5c36e352471cd659bcdb61de08f1722acc3b1"
        # ]
        substrate = SubstrateInterface('http://52.221.95.128:9933')
        # 0x77e03e6e301b3603b4f0f690b338c9922d63c5adb99a9eb4e59f3c0ccb0bcc58
        # 0xcb1daf0b754d9d9930cc104edfb783512dcc343549e4ea352147d106481e30ba
        json_block = substrate.get_chain_block('0xcb1daf0b754d9d9930cc104edfb783512dcc343549e4ea352147d106481e30ba')

        digest_logs = json_block['block']['header'].get('digest', {}).pop('logs', None)
        harvester = PolkascanHarvesterService(None, type_registry='default')
        if len(digest_logs) != 0:
            for dg in digest_logs:
                if dg[0:4] == '0x04':
                    merkle_root = harvester.decode_log_pow(dg)
                elif dg[0:8] == '0x001802':
                    # 0x0018020002000400
                    print(dg[10:12])

        self.assertEqual(merkle_root, '53ab6f561d1d034534fa756ed01d309a60d87106ce7dfc3d37af67f8e9bac32f')
