"""convert app tables to utf8mb4 (fix Chinese text / error 1366)



Revision ID: 003_utf8mb4

Revises: 002_security_ref

Create Date: 2026-04-04



"""



from __future__ import annotations



from typing import Sequence, Union



from alembic import op



revision: str = "003_utf8mb4"

down_revision: Union[str, None] = "002_security_ref"

branch_labels: Union[str, Sequence[str], None] = None

depends_on: Union[str, Sequence[str], None] = None



_UTF8MB4_COLLATE = "utf8mb4_unicode_ci"





def upgrade() -> None:

    # 库默认若为 latin1，此前建表 VARCHAR 无法存中文；连接 charset  alone 不够，须改表字符集。

    for table in ("screening_run", "screening_result", "security_reference"):

        op.execute(

            f"ALTER TABLE `{table}` CONVERT TO CHARACTER SET utf8mb4 COLLATE {_UTF8MB4_COLLATE}"

        )





def downgrade() -> None:

    # 回滚为 latin1 会丢中文，故不在此处自动执行。

    pass


