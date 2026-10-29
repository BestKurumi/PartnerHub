"""Portable launcher contracts; no Docker daemon or third-party Python packages."""
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class DemoLauncherTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("demo_launcher", Path(__file__).with_name("demo.py"))
        self.demo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.demo)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / ".env.example").write_text(
            "COMPOSE_PROJECT_NAME=partner-hub\nHUB_HTTP_PORT=8080\n"
            "GRAFANA_HOST_PORT=3000\nPROMETHEUS_HOST_PORT=9090\n"
            "HUB_IMAGE_TARGET=runtime\nMYSQL_PASSWORD=example\n"
            "MYSQL_ROOT_PASSWORD=example\nPLATFORM1_DB_PASSWORD=example\n"
            "PLATFORM2_DB_PASSWORD=example\nDEMO_TOKEN=example\n"
            "HUB_OPS_TOKEN=example\nGRAFANA_PASSWORD=example\n", encoding="utf-8")

    def test_init_generates_independent_credentials_and_never_overwrites_existing_environment(self):
        self.assertTrue(self.demo.init_env(self.root))
        values = self.demo.read_env(self.root / ".env")
        names = ("MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "PLATFORM1_DB_PASSWORD",
                 "PLATFORM2_DB_PASSWORD", "DEMO_TOKEN", "HUB_OPS_TOKEN", "GRAFANA_PASSWORD")
        credentials = [values[name] for name in names]
        self.assertEqual(len(set(credentials)), 7)
        self.assertTrue(all(len(value) >= 32 and value.isalnum() for value in credentials))
        saved = (self.root / ".env").read_bytes()
        self.assertFalse(self.demo.init_env(self.root))
        self.assertEqual((self.root / ".env").read_bytes(), saved)

    def test_up_builds_sources_in_docker_without_host_go_or_architecture_override(self):
        self.demo.init_env(self.root)
        commands = []
        with patch.object(self.demo, "ROOT", self.root), patch.object(self.demo, "run", side_effect=lambda args, **kw: commands.append(args)):
            self.assertEqual(self.demo.main(["up"]), 0)
        up = next(cmd for cmd in commands if "up" in cmd)
        self.assertEqual(up[:4], ["docker", "compose", "--env-file", str(self.root / ".env")])
        self.assertIn("--build", up)
        self.assertIn("--wait", up)
        self.assertFalse(any("GOARCH" in arg or "local-runtime" in arg for cmd in commands for arg in cmd))
        self.assertTrue(all(cmd[0] == "docker" for cmd in commands))

    def test_down_preserves_volumes_and_uses_explicit_config_from_any_working_directory(self):
        self.demo.init_env(self.root)
        commands = []
        with patch.object(self.demo, "ROOT", self.root), patch.object(self.demo, "run", side_effect=lambda args, **kw: commands.append(args)):
            self.assertEqual(self.demo.main(["down"]), 0)
        self.assertEqual(commands[-1][-1], "down")
        self.assertFalse(any(arg in ("-v", "--volumes") for cmd in commands for arg in cmd))

    def test_urls_use_actual_env_ports_without_printing_credentials(self):
        (self.root / ".env").write_text("HUB_HTTP_PORT=18080\nGRAFANA_HOST_PORT=13000\nPROMETHEUS_HOST_PORT=19090\nDEMO_TOKEN=never-print-this\n", encoding="utf-8")
        import io
        from contextlib import redirect_stdout
        output = io.StringIO()
        with patch.object(self.demo, "ROOT", self.root), redirect_stdout(output):
            self.assertEqual(self.demo.main(["urls"]), 0)
        self.assertIn("http://127.0.0.1:18080/hub/", output.getvalue())
        self.assertIn("http://127.0.0.1:13000", output.getvalue())
        self.assertNotIn("never-print-this", output.getvalue())

    def test_invalid_environment_is_rejected_as_data_not_executed(self):
        path = self.root / ".env"
        path.write_text("DEMO_TOKEN=abc\nDEMO_TOKEN=duplicate\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self.demo.read_env(path)
        path.write_text("HUB_HTTP_PORT=$(touch should-not-exist)\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self.demo.read_env(path)
        self.assertFalse((self.root / "should-not-exist").exists())

    def test_native_init_builds_consistent_dsns_and_preserves_custom_config(self):
        (self.root / "config").mkdir()
        (self.root / "config/native.env.example").write_text("MYSQL_ADDR=127.0.0.1:23306\nHUB_REDIS_ADDR=127.0.0.1:26379\n", encoding="utf-8")
        self.assertTrue(self.demo.init_native(self.root))
        values = self.demo.read_env(self.root / ".env.native")
        self.assertNotIn("HUB_DSN", values)
        resolved = self.demo.native_values(values)
        self.assertIn("hub:" + values["MYSQL_PASSWORD"] + "@tcp(127.0.0.1:23306)/partner_hub?", resolved["HUB_DSN"])
        self.assertIn("mock_p1:" + values["PLATFORM1_DB_PASSWORD"], resolved["PLATFORM1_DSN"])
        saved = (self.root / ".env.native").read_bytes()
        self.assertFalse(self.demo.init_native(self.root))
        self.assertEqual((self.root / ".env.native").read_bytes(), saved)

    def test_native_migration_runs_local_go_without_starting_containers(self):
        (self.root / ".env.native").write_text("MYSQL_ADDR=127.0.0.1:23306\nMYSQL_ROOT_PASSWORD=custom\nMYSQL_PASSWORD=a\nPLATFORM1_DB_PASSWORD=b\nPLATFORM2_DB_PASSWORD=c\nHUB_KAFKA_BROKERS=127.0.0.1:9092\n", encoding="utf-8")
        calls = []
        with patch.object(self.demo, "ROOT", self.root), patch.object(self.demo, "run", side_effect=lambda args, **kw: calls.append((args,kw))):
            self.assertEqual(self.demo.main(["native-migrate"]),0)
        self.assertEqual(calls[0][0], ["go","run","./cmd/migrate","-dir","deploy/mysql","-kafka"])
        self.assertEqual(calls[0][1]["env"]["MYSQL_ADDR"],"127.0.0.1:23306")
        self.assertTrue(all(call[0][0]!="docker" for call in calls))

    def test_native_up_initializes_dependencies_before_starting_processes(self):
        (self.root / ".env.native").write_text("MYSQL_ADDR=127.0.0.1:23306\nMYSQL_PASSWORD=a\nPLATFORM1_DB_PASSWORD=b\nPLATFORM2_DB_PASSWORD=c\nHUB_KAFKA_BROKERS=127.0.0.1:9092\n", encoding="utf-8")
        actions=[]
        with patch.object(self.demo,"ROOT",self.root), patch.object(self.demo,"run",side_effect=lambda args,**kw: actions.append("migrate")), patch.object(self.demo,"native_up",side_effect=lambda values: actions.append("start")):
            self.assertEqual(self.demo.main(["native-up"]),0)
        self.assertEqual(actions,["migrate","start"])

    def test_native_runtime_ignores_inherited_cross_compile_target(self):
        with patch.dict("os.environ", {"GOOS": "linux", "GOARCH": "arm64"}):
            env = self.demo.native_environment({"MYSQL_ADDR": "127.0.0.1:23306"})
        self.assertNotIn("GOOS", env)
        self.assertNotIn("GOARCH", env)
        self.assertEqual(env["MYSQL_ADDR"], "127.0.0.1:23306")

    def test_native_password_is_literal_and_ipv6_is_accepted(self):
        path = self.root / ".env.native"
        path.write_text("MYSQL_ROOT_PASSWORD=abc!#$*'\" space\nHUB_REDIS_ADDR=[::1]:6379\n", encoding="utf-8")
        values = self.demo.read_env(path, literal=True)
        self.assertEqual(values["MYSQL_ROOT_PASSWORD"], "abc!#$*'\" space")
        self.assertEqual(values["HUB_REDIS_ADDR"], "[::1]:6379")

    def test_native_urls_do_not_advertise_unconfigured_observation_services(self):
        import io
        from contextlib import redirect_stdout
        output = io.StringIO()
        with redirect_stdout(output):
            self.demo.native_urls({"NATIVE_GATEWAY_LISTEN": "127.0.0.1:28080"})
        self.assertIn("http://127.0.0.1:28080/hub/", output.getvalue())
        self.assertIn(".env.native", output.getvalue())
        self.assertNotIn(":3000", output.getvalue())
        self.assertNotIn(":9090", output.getvalue())


if __name__ == "__main__":
    unittest.main()
