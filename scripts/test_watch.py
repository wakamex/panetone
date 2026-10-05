import sqlite3
import unittest

from watch import store_problems, task_problems


class TaskProblems(unittest.TestCase):
    def test_reports_tasks_that_are_not_running(self):
        tasks = {
            "outbox": {"state": "running", "last_error": None},
            "wakterm-events": {"state": "retrying", "last_error": "event follower exited"},
        }
        problems = task_problems(tasks)
        self.assertEqual(sorted(problems), ["task:wakterm-events"])
        self.assertIn("retrying: event follower exited", problems["task:wakterm-events"])


class StoreProblems(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        for table, key in [("outbox", "effect_id"), ("inbox", "effect_id"), ("workflows", "request_id")]:
            self.db.execute(f"CREATE TABLE {table} ({key} TEXT, channel TEXT, state TEXT, record_json TEXT, created_at_ms INTEGER)")
        self.db.execute("CREATE TABLE return_deliveries (request_id TEXT, agent_state TEXT, record_json TEXT)")

    def test_reports_failed_unconfirmed_and_stuck_records_only(self):
        rows = [
            ("outbox", "o1", "telegram", "failed", '{"last_error": "Bad Gateway", "body": "reply"}', 0),
            ("outbox", "o2", "telegram", "delivered", '{"body": "fine"}', 0),
            ("outbox", "o3", "signal", "indeterminate", '{"last_error": "closed", "body": "maybe"}', 0),
            ("inbox", "i1", "telegram", "indeterminate", '{"body": "hello"}', 0),
            ("inbox", "i2", "signal", "pending", '{"body": "old"}', 0),
            ("inbox", "i3", "signal", "pending", '{"body": "fresh"}', 999_900_000),
            ("workflows", "w1", None, "indeterminate", '{"command": {"source": "a", "target": "b"}}', 0),
            ("workflows", "w2", None, "failed", '{"command": {"source": "a", "target": "b"}}', 0),
            ("workflows", "w3", None, "indeterminate", '{"command": {"source": "p", "target": "p", "message": "Panetone watch found new problems:\\n- x"}}', 0),
        ]
        for table, key, channel, state, record, created in rows:
            self.db.execute(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?)", (key, channel, state, record, created))
        self.db.execute("INSERT INTO return_deliveries VALUES ('r1', 'indeterminate', '{\"result\": {\"message\": \"x\"}}')")
        problems = store_problems(self.db, 1_000_000_000)
        self.assertEqual(
            sorted(problems),
            ["inbox:i1:indeterminate", "inbox:i2:pending", "outbox:o1", "outbox:o3", "return:r1", "workflow:w1"],
        )
        self.assertIn("Bad Gateway", problems["outbox:o1"])


if __name__ == "__main__":
    unittest.main()
