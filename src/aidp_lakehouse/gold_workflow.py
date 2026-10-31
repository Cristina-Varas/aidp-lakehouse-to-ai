"""Session-local proof gate for notebook 03; starting any build revokes old PASS."""
from copy import deepcopy
from .gold import CONTRACTS_GOLD


class GoldVerification:
    def __init__(self):
        self.reset()

    def reset(self):
        self.first=None
        self.second=None
        self.validated=False

    def build_first(self,spark,config,builder):
        self.reset()  # Revoke old success BEFORE calling anything that might fail.
        try:
            value=builder(spark,config)
            self._require_complete(value)
            self.first=deepcopy(value)
            return value
        except BaseException:
            self.reset()
            raise

    def build_second(self,spark,config,builder):
        self.second=None
        self.validated=False
        if self.first is None:raise ValueError('Complete the first build in this execution before the rerun')
        try:
            value=builder(spark,config)
            self._require_complete(value)
            if self.first['validation']!=value['validation'] or self.first['fingerprints']!=value['fingerprints']:
                raise ValueError('Gold rerun changed business results')
            if any(v['inserted_rows']!=0 for v in value['publication'].values()):
                raise ValueError('Gold rerun unexpectedly inserted rows')
            self.second=deepcopy(value)
            self.validated=True
            return value
        except BaseException:
            # Interrupted/failed reruns must not resurrect earlier verification.
            self.second=None
            self.validated=False
            raise

    @staticmethod
    def _require_complete(value):
        if value.get('validation',{}).get('reconciliation')!='PASS':raise ValueError('Gold reconciliation is incomplete')
        if set(value.get('publication',{}))!=set(CONTRACTS_GOLD) or set(value.get('fingerprints',{}))!=set(CONTRACTS_GOLD):
            raise ValueError('Gold verification must cover all four tables')

    def require_pass(self):
        if not self.validated or self.first is None or self.second is None:
            raise ValueError('GOLD PASS requires successful first build, second build and comparison in this execution')
        self._require_complete(self.first);self._require_complete(self.second)
        if self.first['validation']!=self.second['validation'] or self.first['fingerprints']!=self.second['fingerprints'] or any(
                v['inserted_rows']!=0 for v in self.second['publication'].values()):
            self.validated=False
            raise ValueError('Gold verification state no longer matches')
        return True
