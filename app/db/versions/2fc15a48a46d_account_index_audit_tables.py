"""Account Index Audit tables

Revision ID: 2fc15a48a46d
Revises: a0e3968eb185
Create Date: 2019-06-25 15:44:44.727323

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '2fc15a48a46d'
down_revision = 'a0e3968eb185'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('data_account_audit',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('account_id', sa.String(length=64), nullable=False),
    sa.Column('block_id', sa.Integer(), nullable=False),
    sa.Column('extrinsic_idx', sa.Integer(), nullable=True),
    sa.Column('event_idx', sa.Integer(), nullable=True),
    sa.Column('type_id', sa.Integer(), nullable=False),
    sa.Column('data', sa.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id', 'account_id')
    )
    op.create_index(op.f('ix_data_account_audit_block_id'), 'data_account_audit', ['block_id'], unique=False)
    op.create_table('data_account_index_audit',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('account_index_id', sa.Integer(), nullable=True),
    sa.Column('account_id', sa.String(length=64), nullable=True),
    sa.Column('block_id', sa.Integer(), nullable=False),
    sa.Column('extrinsic_idx', sa.Integer(), nullable=True),
    sa.Column('event_idx', sa.Integer(), nullable=True),
    sa.Column('type_id', sa.Integer(), nullable=False),
    sa.Column('data', sa.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_data_account_index_audit_account_id'), 'data_account_index_audit', ['account_id'], unique=False)
    op.create_index(op.f('ix_data_account_index_audit_account_index_id'), 'data_account_index_audit', ['account_index_id'], unique=False)
    op.create_index(op.f('ix_data_account_index_audit_block_id'), 'data_account_index_audit', ['block_id'], unique=False)
    op.add_column('data_account', sa.Column('balance', sa.Numeric(precision=65, scale=0), nullable=False))
    op.add_column('data_account', sa.Column('count_reaped', sa.Integer(), nullable=True))
    op.add_column('data_account', sa.Column('is_contract', sa.Boolean(), nullable=True))
    op.add_column('data_account', sa.Column('is_nominator', sa.Boolean(), nullable=True))
    op.add_column('data_account', sa.Column('is_reaped', sa.Boolean(), nullable=True))
    op.add_column('data_account', sa.Column('is_validator', sa.Boolean(), nullable=True))
    op.add_column('data_account', sa.Column('updated_at_block', sa.Integer(), nullable=False))
    op.alter_column('data_account', 'created_at_block',
               existing_type=mysql.INTEGER(display_width=11),
               nullable=False)
    op.drop_column('data_account', 'balance_at_creation')
    op.drop_column('data_account', 'reaped')
    op.drop_column('data_account', 'referenced_at_block')
    op.drop_column('data_account', 'created_at_event')
    op.drop_column('data_account', 'reaped_at_block')
    op.drop_column('data_account', 'created_at_extrinsic')
    op.add_column('data_account_index', sa.Column('account_id', sa.String(length=64), nullable=True))
    op.add_column('data_account_index', sa.Column('is_reclaimable', sa.Boolean(), nullable=True))
    op.add_column('data_account_index', sa.Column('is_reclaimed', sa.Boolean(), nullable=True))
    op.add_column('data_account_index', sa.Column('short_address', sa.String(length=24), nullable=True))
    op.add_column('data_account_index', sa.Column('updated_at_block', sa.Integer(), nullable=False))
    op.alter_column('data_account_index', 'created_at_block',
               existing_type=mysql.INTEGER(display_width=11),
               nullable=False)
    op.create_index(op.f('ix_data_account_index_account_id'), 'data_account_index', ['account_id'], unique=False)
    op.create_index(op.f('ix_data_account_index_short_address'), 'data_account_index', ['short_address'], unique=False)
    op.drop_index('ix_data_account_index_account', table_name='data_account_index')
    op.drop_column('data_account_index', 'reaped')
    op.drop_column('data_account_index', 'referenced_at_block')
    op.drop_column('data_account_index', 'created_at_event')
    op.drop_column('data_account_index', 'account_at_creation')
    op.drop_column('data_account_index', 'account')
    op.drop_column('data_account_index', 'reaped_at_block')
    op.drop_column('data_account_index', 'created_at_extrinsic')
    op.add_column('data_block', sa.Column('count_accounts', sa.Integer(), nullable=False))
    op.add_column('data_block', sa.Column('count_accounts_reaped', sa.Integer(), nullable=False))
    op.add_column('data_block', sa.Column('count_contracts_new', sa.Integer(), nullable=False))
    op.add_column('data_block', sa.Column('count_log', sa.Integer(), nullable=False))
    op.add_column('data_block', sa.Column('count_sessions_new', sa.Integer(), nullable=False))
    op.add_column('data_block_total', sa.Column('session_id', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('data_block_total', 'session_id')
    op.drop_column('data_block', 'count_sessions_new')
    op.drop_column('data_block', 'count_log')
    op.drop_column('data_block', 'count_contracts_new')
    op.drop_column('data_block', 'count_accounts_reaped')
    op.drop_column('data_block', 'count_accounts')
    op.add_column('data_account_index', sa.Column('created_at_extrinsic', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True))
    op.add_column('data_account_index', sa.Column('reaped_at_block', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True))
    op.add_column('data_account_index', sa.Column('account', mysql.VARCHAR(length=64), nullable=True))
    op.add_column('data_account_index', sa.Column('account_at_creation', mysql.VARCHAR(length=64), nullable=True))
    op.add_column('data_account_index', sa.Column('created_at_event', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True))
    op.add_column('data_account_index', sa.Column('referenced_at_block', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True))
    op.add_column('data_account_index', sa.Column('reaped', mysql.TINYINT(display_width=1), autoincrement=False, nullable=True))
    op.create_index('ix_data_account_index_account', 'data_account_index', ['account'], unique=False)
    op.drop_index(op.f('ix_data_account_index_short_address'), table_name='data_account_index')
    op.drop_index(op.f('ix_data_account_index_account_id'), table_name='data_account_index')
    op.alter_column('data_account_index', 'created_at_block',
               existing_type=mysql.INTEGER(display_width=11),
               nullable=True)
    op.drop_column('data_account_index', 'updated_at_block')
    op.drop_column('data_account_index', 'short_address')
    op.drop_column('data_account_index', 'is_reclaimed')
    op.drop_column('data_account_index', 'is_reclaimable')
    op.drop_column('data_account_index', 'account_id')
    op.add_column('data_account', sa.Column('created_at_extrinsic', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True))
    op.add_column('data_account', sa.Column('reaped_at_block', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True))
    op.add_column('data_account', sa.Column('created_at_event', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True))
    op.add_column('data_account', sa.Column('referenced_at_block', mysql.INTEGER(display_width=11), autoincrement=False, nullable=True))
    op.add_column('data_account', sa.Column('reaped', mysql.TINYINT(display_width=1), autoincrement=False, nullable=True))
    op.add_column('data_account', sa.Column('balance_at_creation', mysql.DECIMAL(precision=65, scale=0), nullable=False))
    op.alter_column('data_account', 'created_at_block',
               existing_type=mysql.INTEGER(display_width=11),
               nullable=True)
    op.drop_column('data_account', 'updated_at_block')
    op.drop_column('data_account', 'is_validator')
    op.drop_column('data_account', 'is_reaped')
    op.drop_column('data_account', 'is_nominator')
    op.drop_column('data_account', 'is_contract')
    op.drop_column('data_account', 'count_reaped')
    op.drop_column('data_account', 'balance')
    op.drop_index(op.f('ix_data_account_index_audit_block_id'), table_name='data_account_index_audit')
    op.drop_index(op.f('ix_data_account_index_audit_account_index_id'), table_name='data_account_index_audit')
    op.drop_index(op.f('ix_data_account_index_audit_account_id'), table_name='data_account_index_audit')
    op.drop_table('data_account_index_audit')
    op.drop_index(op.f('ix_data_account_audit_block_id'), table_name='data_account_audit')
    op.drop_table('data_account_audit')
    # ### end Alembic commands ###
