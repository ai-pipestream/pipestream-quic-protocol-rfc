import unittest

from wire import (
    STATUS_COMPLETE,
    STATUS_PENDING,
    STATUS_PROCESSING,
    WireError,
    capabilities,
    checkpoint,
    decode_cbor,
    encode_cbor,
    entity_frame,
    parse_entity_frame,
    parse_checkpoint,
    parse_status,
    parse_ucf,
    status,
    validate_capabilities,
    validate_transitions,
)


class WireTest(unittest.TestCase):
    def test_cbor_is_deterministic_and_round_trips(self):
        value = {"z": 4, "a": True, "payload": b"abc"}
        encoded = encode_cbor(value)
        self.assertEqual(value, decode_cbor(encoded))
        self.assertEqual(encoded, encode_cbor({"payload": b"abc", "a": True, "z": 4}))

    def test_default_capabilities_validate(self):
        frame_type, payload = parse_ucf(capabilities())
        self.assertEqual(0x80, frame_type)
        self.assertTrue(validate_capabilities(payload)["layer0-core"])

    def test_entity_round_trip(self):
        header, payload = parse_entity_frame(entity_frame(9, b"payload", content_type="text/plain"))
        self.assertEqual(9, header["entity-id"])
        self.assertEqual(b"payload", payload)

    def test_status_round_trip(self):
        frame_type, payload = parse_ucf(status(7, STATUS_PROCESSING))
        self.assertEqual(0x50, frame_type)
        self.assertEqual(STATUS_PROCESSING, parse_status(payload)["state"])

    def test_checkpoint_round_trip(self):
        frame_type, payload = parse_ucf(checkpoint("barrier-7", 4, 8, acknowledgement=True))
        self.assertEqual(0x81, frame_type)
        self.assertEqual(1, parse_checkpoint(payload)["flags"])

    def test_valid_transition_transcript(self):
        validate_transitions([
            status(7, STATUS_PENDING),
            status(7, STATUS_PROCESSING),
            status(7, STATUS_COMPLETE),
        ])

    def test_terminal_transition_is_rejected(self):
        with self.assertRaisesRegex(WireError, "invalid transition"):
            validate_transitions([
                status(7, STATUS_PENDING),
                status(7, STATUS_PROCESSING),
                status(7, STATUS_COMPLETE),
                status(7, STATUS_PROCESSING),
            ])


if __name__ == "__main__":
    unittest.main()
