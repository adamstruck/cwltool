from __future__ import print_function

import logging
import os
import subprocess
import tempfile
import time
import unittest

from common_test_util import SimpleServerTest, popen

class TestConformance(SimpleServerTest):
    def test_conformance(self):
        tmpdir = tempfile.mkdtemp(dir=self.tmpdir,
                                  prefix="v1.0_ctest_")
        ctest_def = os.path.join(
            self.testdir,
            "../../cwltool/schemas/v1.0/conformance_test_v1.0.yaml"
        )
        tool_entry = os.path.join(self.testdir,
                                  "../cwltool-tes")
        cmd = ["cwltest", "--test", ctest_def, "--basedir", tmpdir,
               "--tool", tool_entry, "-n", "1", "-j", "10"]
        process = popen(cmd,
                        cwd=os.path.join(self.testdir,
                                         "../../cwltool/schemas/v1.0")
        )
        process.wait()
        assert process.returncode == 0
