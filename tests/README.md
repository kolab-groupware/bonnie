Running Bonnie Tests
====================

The test scripts are based on the `twisted trial` package which can be
installed from the `python-twisted-core` RPM package.

Run the tests from the bonnie root directory with the following command:

```
$ export PYTHONPATH=.:/usr/lib/python2.6/site-packages
$ trial tests.{unit|functional}.<test-file-name>.<testClassName>
```

So for example

```
$ trial tests.unit.test-001-utils.TestBonnieUtils
$ trial tests.functional.test-001-login.TestBonnieLogin
```

Both the unit tests as well as the functional tests make the following
assumptions regarding the Kolab environment on the host they're run:

 * Kolab standard single-host setup running the domain 'example.org'
 * A Kolab user named John Doe <john.doe@example.org> exists
 * The Elasticsearch service is running
 * No Bonnie processes are running or connected to ZMQ
