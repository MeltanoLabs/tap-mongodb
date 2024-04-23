"""Test utilities for tap-mongodb.

This module contains things that are outside the scope of conftest.py (which is really just for Pytest fixtures).
"""

from singer_sdk.testing.runners import TapTestRunner


class MongoDBTestRunner(TapTestRunner):
    """MongoDB Tap Test Runner.

    This implementation is lifted from the MeltanoLabs/tap-postgres PostgresTestRunner:
    https://github.com/MeltanoLabs/tap-postgres/blob/f925d01006a4d022b1d57a01ed68eb14599f6599/tests/test_core.py#L414
    """

    def run_sync_dry_run(self) -> bool:
        """Run sync.

        Returns:
            bool: True
        """
        new_tap = self.new_tap()
        new_tap.sync_all()
        return True
