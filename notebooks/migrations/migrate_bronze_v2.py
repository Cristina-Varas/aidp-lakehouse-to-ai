# Paste this entire file into one Python cell in AIDP lakehouse_to_ai.
# Install aidp_lakehouse_to_ai-0.2.2-py3-none-any.whl on the attached compute first.
# Default mode ONLY inspects the platform and writes local restoration artifacts.
from datetime import datetime, timezone
from pathlib import Path
import json
from aidp_lakehouse import __version__
from aidp_lakehouse.bronze_migration import prepare, migrate, restore

assert __version__ == '0.2.2', 'Install the 0.2.2 wheel and restart the session'
MODE = 'prepare'  # Explicit later choices: 'apply' or 'restore'. Never automatic.
# For apply/restore, replace this with the EXACT directory printed by prepare.
# Retain/download the backup outside the notebook/compute before applying changes.
BACKUP_DIRECTORY = str(Path.home() / 'aidp-bronze-backups' /
                       datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
REVIEWED_BACKUP_AND_PERMISSIONS = False
CONFIRMED_WORKSPACE = 'lakehouse_to_ai'  # Verify this in the AIDP UI before execution.
assert CONFIRMED_WORKSPACE == 'lakehouse_to_ai'
if MODE == 'prepare':
    result = prepare(spark, BACKUP_DIRECTORY)
elif MODE in ('apply', 'restore'):
    if not REVIEWED_BACKUP_AND_PERMISSIONS:
        raise ValueError('Review/export definitions, ownership, grants and dependencies; retain backup first')
    result = (migrate if MODE == 'apply' else restore)(spark, BACKUP_DIRECTORY)
else:
    raise ValueError('MODE must be prepare, apply or restore')
print(json.dumps(result, indent=2))
