"""Local configuration and TLS checks; no provider or container is contacted."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import ssl
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/check_development_startup.py"
SPEC = importlib.util.spec_from_file_location("development_preflight", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
preflight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(preflight)


class PreflightDraftTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bounds_path = self.root / "bounds.json"

    def write_bounds(self, **changes: object) -> dict[str, object]:
        values: dict[str, object] = {
            "allow_images": False,
            "max_output_tokens": 512,
            "max_request_bytes": 32_768,
            "max_tool_calls": 1,
        }
        values.update(changes)
        self.bounds_path.write_text(json.dumps(values), encoding="utf-8")
        self.bounds_path.chmod(0o600)
        return values

    def listener(self, cert: Path, key: Path) -> SimpleNamespace:
        return SimpleNamespace(
            control_certificate=cert,
            control_private_key=key,
            preview_certificate=cert,
            preview_private_key=key,
        )

    def test_bounds_allow_only_explicit_nonsecret_fields(self) -> None:
        self.write_bounds()
        result = preflight.load_provider_bounds(self.bounds_path)
        self.assertEqual(result["max_output_tokens"], 512)
        self.assertNotIn("api_key", result)
        self.assertNotIn("base_url", result)
        self.assertNotIn("model", result)

        self.write_bounds(api_key="must-not-be-accepted")
        with self.assertRaisesRegex(preflight.PreflightError, "PROVIDER_BOUNDS_INVALID"):
            preflight.load_provider_bounds(self.bounds_path)

    def test_bounds_reject_duplicates_and_boolean_integer_coercion(self) -> None:
        self.bounds_path.write_text(
            '{"allow_images":false,"allow_images":true,"max_output_tokens":1,'
            '"max_request_bytes":32768,"max_tool_calls":1}', encoding="utf-8",
        )
        self.bounds_path.chmod(0o600)
        with self.assertRaisesRegex(preflight.PreflightError, "PROVIDER_BOUNDS_INVALID"):
            preflight.load_provider_bounds(self.bounds_path)

        self.write_bounds(max_tool_calls=True)
        with self.assertRaisesRegex(preflight.PreflightError, "PROVIDER_BOUNDS_INVALID"):
            preflight.load_provider_bounds(self.bounds_path)

    def test_images_require_provider_support_but_small_requests_remain_valid(self) -> None:
        bounds = {
            "allow_images": False,
            "max_output_tokens": 512,
            "max_request_bytes": 32_768,
            "max_tool_calls": 1,
        }
        dev = SimpleNamespace(model_images=True, max_tool_calls=32)
        with self.assertRaisesRegex(preflight.PreflightError, "IMAGE_CAPABILITY_MISMATCH"):
            preflight.verify_consistency(dev, bounds, 1_048_576)
        bounds["allow_images"] = True
        self.assertEqual(preflight.verify_consistency(dev, bounds, 32_768), 32_768)
        dev.model_images = False
        self.assertEqual(preflight.verify_consistency(dev, bounds, 1_048_576), 32_768)

    def test_effective_request_budget_uses_lower_ceiling(self) -> None:
        development = SimpleNamespace(
            preview=SimpleNamespace(listener=None), browser=object(),
            model_images=False, max_steps=8, max_tool_calls=32,
        )
        bounds = {
            "allow_images": False, "max_output_tokens": 512,
            "max_request_bytes": 1_048_576, "max_tool_calls": 1,
        }
        with patch.object(preflight, "verify_browser"):
            report = preflight._build_report(development, object(), bounds, "b" * 64, 262_144)
        self.assertEqual(report["budgets"]["effective_max_request_bytes"], 262_144)

    def test_per_response_tool_cap_may_exceed_run_total_cap(self) -> None:
        bounds = {
            "allow_images": False,
            "max_output_tokens": 512,
            "max_request_bytes": 32_768,
            "max_tool_calls": 4,
        }
        dev = SimpleNamespace(model_images=False, max_tool_calls=2)
        self.assertEqual(preflight.verify_consistency(dev, bounds, 262_144), 32_768)

    def test_prompt_hash_only_is_returned(self) -> None:
        prompt = self.root / "prompt.txt"
        prompt.write_text("private fixture prompt", encoding="utf-8")
        report = preflight._build_report(
            SimpleNamespace(
                preview=SimpleNamespace(listener=None), browser=None,
                model_images=False, max_steps=8, max_tool_calls=32,
            ),
            object(),
            {
                "allow_images": False, "max_output_tokens": 512,
                "max_request_bytes": 32_768, "max_tool_calls": 1,
            },
            preflight.prompt_sha256(prompt),
            262_144,
        )
        encoded = json.dumps(report)
        self.assertEqual(
            report["prompt_sha256"], hashlib.sha256(b"private fixture prompt").hexdigest()
        )
        self.assertNotIn("private fixture prompt", encoded)
        self.assertEqual(report["state"], "blocked")
        self.assertIn("image_host_product", report["unverified"])
        self.assertEqual(report["budgets"]["effective_max_request_bytes"], 32_768)
        self.assertEqual(report["budgets"]["development_run_timeout_seconds"], 300)
        self.assertEqual(report["budgets"]["development_tool_timeout_seconds"], 30)

    def test_static_ready_still_marks_runtime_and_product_unverified(self) -> None:
        development = SimpleNamespace(
            preview=SimpleNamespace(listener=object()),
            browser=object(), model_images=False, max_steps=8, max_tool_calls=32,
        )
        bounds = {
            "allow_images": False, "max_output_tokens": 512,
            "max_request_bytes": 32_768, "max_tool_calls": 1,
        }
        with (
            patch.object(preflight, "verify_tls_listener"),
            patch.object(preflight, "verify_browser"),
        ):
            report = preflight._build_report(development, object(), bounds, "a" * 64, 262_144)
        self.assertEqual(report["state"], "static_ready")
        self.assertIn("broker_runtime", report["unverified"])
        self.assertIn("docker_runtime", report["unverified"])
        self.assertIn("image_host_product", report["unverified"])

    def test_tls_file_permission_and_cert_loading_are_local_only(self) -> None:
        cert = self.root / "cert.pem"
        key = self.root / "key.pem"
        cert.write_text("not a certificate", encoding="ascii")
        key.write_text("not a key", encoding="ascii")
        cert.chmod(0o600)
        key.chmod(0o600)
        with patch.object(ssl.SSLContext, "load_cert_chain", return_value=None) as load:
            preflight.verify_tls_listener(self.listener(cert, key))
            self.assertEqual(load.call_count, 2)
        key.chmod(0o640)
        with self.assertRaisesRegex(preflight.PreflightError, "TLS_FILE_INVALID"):
            preflight.verify_tls_listener(self.listener(cert, key))

    def test_tls_parse_failure_is_sanitized(self) -> None:
        cert = self.root / "cert.pem"
        key = self.root / "key.pem"
        cert.write_text("fixture", encoding="ascii")
        key.write_text("fixture", encoding="ascii")
        cert.chmod(0o600)
        key.chmod(0o600)
        with self.assertRaisesRegex(preflight.PreflightError, "TLS_CERTIFICATE_INVALID") as raised:
            preflight.verify_tls_listener(self.listener(cert, key))
        self.assertNotIn(str(self.root), str(raised.exception))

    def test_browser_ca_digest_and_seccomp_file(self) -> None:
        ca = self.root / "ca.pem"
        seccomp = self.root / "profile.json"
        payload = b"local fixture CA bytes"
        ca.write_bytes(payload)
        seccomp.write_text("{}", encoding="ascii")
        config = SimpleNamespace(
            ca_path=ca, ca_sha256=hashlib.sha256(payload).hexdigest(), seccomp_path=seccomp,
        )
        preflight.verify_browser(config)
        config.ca_sha256 = "0" * 64
        with self.assertRaisesRegex(preflight.PreflightError, "BROWSER_CA_HASH_MISMATCH"):
            preflight.verify_browser(config)
        config.ca_sha256 = hashlib.sha256(payload).hexdigest()
        seccomp.unlink()
        with self.assertRaisesRegex(preflight.PreflightError, "SECCOMP_FILE_MISSING"):
            preflight.verify_browser(config)

    def test_real_config_loaders_and_tls_without_network_or_database(self) -> None:
        from krim_agent.development.configuration import (
            DevelopmentBrowserConfiguration,
            DevelopmentConfiguration,
            DevelopmentListenerConfiguration,
            DevelopmentPreviewConfiguration,
        )
        from krim_agent.sandbox.configuration import SandboxConfiguration
        from tests.coding_agent.prepare_preview_listener import certificates

        certificates(self.root)
        ca = self.root / "tls.crt"
        profile = self.root / "seccomp.json"
        profile.write_text("{}")
        config = DevelopmentConfiguration(
            image_id="sha256:" + "a" * 64,
            broker_environments_root=self.root / "environments",
            private_state_root=self.root / "private",
            model_images=True,
            preview=DevelopmentPreviewConfiguration(
                control_origin="https://control.example",
                control_cookie_domain="control.example",
                preview_domain="preview.example",
                listener=DevelopmentListenerConfiguration(
                    preview_port=443,
                    control_certificate=ca,
                    preview_certificate=ca,
                    control_private_key=self.root / "tls.key",
                    preview_private_key=self.root / "tls.key",
                ),
            ),
            browser=DevelopmentBrowserConfiguration(
                image_id="sha256:" + "b" * 64,
                ca_path=ca,
                ca_sha256=hashlib.sha256(ca.read_bytes()).hexdigest(),
                seccomp_path=profile,
            ),
        )
        development = self.root / "development.json"
        development.write_text(config.model_dump_json())
        development.chmod(0o600)
        sandbox = self.root / "sandbox.json"
        sandbox.write_text(SandboxConfiguration().model_dump_json())
        sandbox.chmod(0o600)
        self.write_bounds(allow_images=True)
        prompt = self.root / "prompt.md"
        prompt.write_text("existing product requirement")
        args = argparse.Namespace(
            project_root=SCRIPT.parent.parent,
            development_config=development,
            sandbox_config=sandbox,
            provider_bounds=self.bounds_path,
            host_max_request_bytes=262_144,
            prompt=prompt,
        )
        with patch("socket.socket", side_effect=AssertionError("no networking")):
            report, status = preflight.run_preflight(args)
        self.assertEqual(status, 0, report)
        self.assertEqual(report["state"], "static_ready")
        self.assertEqual(report["budgets"]["effective_max_request_bytes"], 32_768)
        self.assertTrue(report["budgets"]["actual_wire_request_checked_at_runtime"])
        self.assertEqual(list(self.root.glob("*.sqlite")), [])
        development.chmod(0o644)
        failed, status = preflight.run_preflight(args)
        self.assertEqual(status, 2)
        self.assertEqual(failed["checks"][0]["code"], "DEVELOPMENT_CONFIG_INVALID")

    def test_preflight_errors_do_not_echo_paths_or_values(self) -> None:
        args = argparse.Namespace(
            project_root=self.root / "missing-project",
            development_config=self.root / "private-development.json",
            sandbox_config=self.root / "private-sandbox.json",
            provider_bounds=self.bounds_path,
            host_max_request_bytes=262_144,
            prompt=self.root / "private-prompt.txt",
        )
        report, status = preflight.run_preflight(args)
        self.assertEqual(status, 2)
        encoded = json.dumps(report)
        self.assertNotIn(str(self.root), encoded)
        self.assertEqual(set(report["checks"][0]), {"code", "status"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
