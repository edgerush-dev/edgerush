"""The request future's head is found first where rustc put it first, and not where its
awaited future went ahead of it, as rustc printed both (14 §3)."""

import contextlib
import io
import unittest

import layout

FUTURE = "{async fn body of edgerush_proxy::serve::respond::<impl edgerush_proxy::Worker>::handle_head<edgerush_proxy::raw::RawHead>()}"
AWAITED = "{async fn body of edgerush_proxy::serve::respond::<impl edgerush_proxy::Worker>::respond_to<edgerush_proxy::raw::RawHead>()}"

# As rustc printed them, the head first (respond_to's future 6,000 bytes) and after it
# (6,016 bytes); cut short after the variants that matter.
FIRST = f"""\
print-type-size type: `{FUTURE}`: 6560 bytes, alignment: 16 bytes
print-type-size     discriminant: 1 bytes
print-type-size     variant `Unresumed`: 6544 bytes
print-type-size         upvar `.head`: 320 bytes, offset: 0 bytes, alignment: 16 bytes
print-type-size         padding: 6016 bytes
print-type-size         upvar `.listener`: 8 bytes, alignment: 8 bytes
print-type-size     variant `Suspend0`: 6550 bytes
print-type-size         upvar `.head`: 320 bytes, offset: 0 bytes, alignment: 16 bytes
print-type-size         local `.came_in`: 16 bytes
print-type-size         local `.__awaitee`: 6000 bytes, type: {AWAITED}
"""
AFTER = f"""\
print-type-size type: `{FUTURE}`: 6576 bytes, alignment: 16 bytes
print-type-size     discriminant: 1 bytes
print-type-size     variant `Unresumed`: 6560 bytes
print-type-size         padding: 6015 bytes
print-type-size         upvar `.head`: 320 bytes, alignment: 16 bytes
print-type-size         padding: 16 bytes
print-type-size     variant `Suspend0`: 6566 bytes
print-type-size         local `.__awaitee`: 6016 bytes, offset: 0 bytes, alignment: 16 bytes, type: {AWAITED}
print-type-size         upvar `.head`: 320 bytes
"""
OTHER = """\
print-type-size type: `{async fn body of other()}`: 64 bytes, alignment: 8 bytes
print-type-size     discriminant: 1 bytes
print-type-size     variant `Unresumed`: 8 bytes
print-type-size         upvar `.head`: 8 bytes, offset: 0 bytes, alignment: 8 bytes
"""


def checked(text, field=".head"):
    """layout.py's exit status for `text`, what it says of a failure kept out of the run."""
    with contextlib.redirect_stderr(io.StringIO()):
        return layout.main(["layout.py", FUTURE, field], text)


class FirstField(unittest.TestCase):
    def test_the_head_first_passes(self):
        self.assertEqual(layout.first_field(FIRST, FUTURE), (".head", 0))
        self.assertEqual(checked(FIRST), 0)

    def test_the_head_after_the_awaited_future_fails(self):
        self.assertEqual(layout.first_field(AFTER, FUTURE), ("padding of 6015 bytes", 0))
        self.assertEqual(checked(AFTER), 1)

    def test_another_field_first_fails(self):
        self.assertEqual(checked(FIRST, ".body"), 1)

    def test_a_future_not_printed_is_not_passed(self):
        self.assertEqual(checked(OTHER), 2)
        self.assertEqual(checked(""), 2)

    def test_only_the_future_named_is_read(self):
        self.assertEqual(checked(OTHER + AFTER), 1)
        self.assertEqual(checked(AFTER + OTHER), 1)
        self.assertEqual(checked(FIRST + OTHER), 0)

    def test_a_future_with_no_unresumed_variant_is_not_passed(self):
        cut = FIRST.split("print-type-size     variant `Unresumed`")[0]
        self.assertEqual(checked(cut + OTHER), 2)

    def test_the_variant_it_starts_in_is_the_one_read(self):
        suspended_first = FIRST.replace("`Unresumed`", "`Before`").replace(
            "`Suspend0`", "`Unresumed`"
        )
        self.assertEqual(layout.first_field(suspended_first, FUTURE), (".head", 0))
        awaited_first = AFTER.replace("`Unresumed`", "`Before`").replace(
            "`Suspend0`", "`Unresumed`"
        )
        self.assertEqual(layout.first_field(awaited_first, FUTURE), (".__awaitee", 0))


if __name__ == "__main__":
    unittest.main()
