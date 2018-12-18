#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 12-JUL-2018
updated_at: 12-JUL-2018

"""
from datetime import datetime, timedelta
import unittest
from mock import MagicMock, patch



class JobTrackerTests(unittest.TestCase):

    date_format = '%Y%m%d%H%M%S'

    @patch("samba_chassis.jobs._Session")
    def test_job_creation(self, s):
        from samba_chassis import jobs
        jobs._config = MagicMock(name="tasks", project="project", user="x", password="x",
                                 host_ip='127.0.0.1', port=3306, database="jobs")
        jobs.create("Testing")
        self.assertEqual(s().add.call_count, 1)
        self.assertEqual(s().commit.call_count, 1)

    @patch("samba_chassis.jobs._Session")
    def test_job_representation(self, s):
        from samba_chassis import jobs
        print(jobs.Job(status="Testing", id=1))

    @patch("samba_chassis.jobs._Session")
    def test_job_update(self, s):
        from samba_chassis import jobs
        s().query().filter().one.return_value = MagicMock(finished=False, _sa_instance_state=None)
        r = jobs.update("1", new_status="testing")
        self.assertEqual(s().query().filter().one.call_count, 1)
        self.assertEqual(r["status"], "testing")

    @patch("samba_chassis.jobs._Session")
    def test_job_update_new_data(self, s):
        from samba_chassis import jobs
        s().query().filter().one.return_value = MagicMock(finished=False, data={"um": "one"}, _sa_instance_state=None)
        r = jobs.update("1", new_status="testing", new_data={"dois": "two"})
        self.assertEqual(r["data"], {"dois": "two"})

    @patch("samba_chassis.jobs._Session")
    def test_job_update_new_data_append(self, s):
        from samba_chassis import jobs
        s().query().filter().one.return_value = MagicMock(finished=False, data={"um": "one"}, _sa_instance_state=None)
        r = jobs.update("1", new_data={"dois": "two"}, append_data=True)
        self.assertEqual(r["data"], {"um": "one", "dois": "two"})

    @patch("samba_chassis.jobs._Session")
    def test_job_update_end(self, s):
        from samba_chassis import jobs
        s().query().filter().one.return_value = MagicMock(finished=False, data={"um": "one"}, _sa_instance_state=None)
        r = jobs.update("1", end=True)
        self.assertEqual(r["finished"], True)

    @patch("samba_chassis.jobs._Session")
    def test_job_update_end_finished(self, s):
        from samba_chassis import jobs
        s().query().filter().one.return_value = MagicMock(finished=True, data={"um": "one"}, _sa_instance_state=None)

    @patch("samba_chassis.jobs._Session")
    def test_end(self, s):
        from samba_chassis import jobs
        s().query().filter().one.return_value = MagicMock(finished=False, data={"um": "one"}, _sa_instance_state=None)
        r = jobs.end(1)
        self.assertEqual(r["finished"], True)

        s().query().filter().one.return_value = MagicMock(finished=True, data={"um": "one"}, _sa_instance_state=None)
        r = jobs.end(1)
        self.assertEqual(r["finished"], True)

    @patch("samba_chassis.jobs._Session")
    def test_ready(self, s):
        from samba_chassis import jobs
        s().query().limit().all.return_value = "1"
        r = jobs.ready()
        self.assertEqual(r, {"DATABASE": "OK"})

        s().query().limit().all.return_value = "two"
        r = jobs.ready()
        self.assertEqual(r, {"DATABASE": "ERROR"})

        s().query().limit().all.return_value = Exception()
        r = jobs.ready()
        self.assertEqual(r, {"DATABASE": "ERROR"})

    @patch("samba_chassis.jobs._Session")
    def test_delete(self, s):
        from samba_chassis import jobs
        jobs.delete(1)
        self.assertEqual(s().delete.call_count, 1)
        self.assertEqual(s().commit.call_count, 1)

    @patch("samba_chassis.jobs._Session")
    def test_delete_deleted(self, s):
        from samba_chassis import jobs
        from sqlalchemy.orm.exc import NoResultFound
        s().query().filter().one.side_effect = NoResultFound()
        jobs.delete(1)
        self.assertEqual(s().delete.call_count, 0)
        self.assertEqual(s().commit.call_count, 1)

    @patch("samba_chassis.jobs._Session")
    def test_get(self, s):
        from samba_chassis import jobs
        s().query().filter().one.return_value = MagicMock(test=True, _sa_instance_state=None)
        r = jobs.get(1)
        self.assertTrue(r["test"])

    @patch("samba_chassis.jobs._Session")
    def test_get_error(self, s):
        from samba_chassis import jobs
        from sqlalchemy.exc import InvalidRequestError
        s().query().filter().one.side_effect = InvalidRequestError()
        r = jobs.get(1)
        self.assertIsNone(r)
