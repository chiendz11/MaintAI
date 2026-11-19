import json
import unittest

from maintai_automation.normalize import NormalizeError, normalize_inputs


class NormalizeInputsTest(unittest.TestCase):
    def test_webhook_raw_and_wrapped_payload_decodes_and_deduplicates(self):
        minio_event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "maintai-approved"},
                        "object": {
                            "key": "datasets%2Fapproved%2Fai4i-v1.manifest.json",
                            "versionId": "vid-manifest-1",
                        },
                    }
                },
                {
                    "s3": {
                        "bucket": {"name": "maintai-approved"},
                        "object": {
                            "key": "datasets/approved/ai4i-v1.manifest.json",
                            "versionId": "vid-manifest-1",
                        },
                    }
                },
            ]
        }
        records = normalize_inputs(event_body=json.dumps(minio_event))
        self.assertEqual(
            records,
            [
                {
                    "slot": "ds-0",
                    "bucket": "maintai-approved",
                    "key": "datasets/approved/ai4i-v1.manifest.json",
                    "version_id": "vid-manifest-1",
                    "trigger_source": "webhook",
                }
            ],
        )

        wrapped = normalize_inputs(event_body=json.dumps({"body": minio_event}))
        self.assertEqual(wrapped[0]["key"], "datasets/approved/ai4i-v1.manifest.json")

    def test_manual_submit_accepts_relative_key_and_s3_uri(self):
        plain = normalize_inputs(
            event_body="{}",
            dataset_manifest="datasets/approved/ai4i-v2.manifest.json",
            manifest_version_id="v42",
        )
        self.assertEqual(plain[0]["trigger_source"], "manual")
        self.assertEqual(plain[0]["bucket"], "maintai-approved")
        self.assertEqual(plain[0]["key"], "datasets/approved/ai4i-v2.manifest.json")
        self.assertEqual(plain[0]["version_id"], "v42")

        s3_uri = normalize_inputs(
            dataset_manifest="s3://maintai-approved/datasets/approved/ai4i-v3.manifest.json"
        )
        self.assertEqual(s3_uri[0]["key"], "datasets/approved/ai4i-v3.manifest.json")
        self.assertEqual(s3_uri[0]["trigger_source"], "manual")

    def test_rejects_invalid_prefix_suffix_bucket_or_empty_inputs(self):
        with self.assertRaises(NormalizeError):
            normalize_inputs()
        with self.assertRaises(NormalizeError):
            normalize_inputs(dataset_manifest="datasets/raw/bad.csv")
        with self.assertRaises(NormalizeError):
            normalize_inputs(dataset_manifest="s3://maintai-raw/datasets/approved/x.manifest.json")
        with self.assertRaises(NormalizeError):
            normalize_inputs(event_body='{"Records": []}')
        with self.assertRaises(NormalizeError):
            normalize_inputs(event_body="not-json")
