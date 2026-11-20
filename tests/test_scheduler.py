import unittest


class SchedulingTests(unittest.TestCase):
    def test_probes_large_then_reverses_when_throughput_falls(self):
        from scheduler import AdaptiveMix
        mix = AdaptiveMix(64, 16, window=10)
        signals = (0, 0, (5000, 300))
        for start, target, received, expected in [
                (0, 16, 1000, 20), (10, 20, 1300, 24),
                (20, 24, 900, 20), (30, 20, 1300, 20),
                (40, 20, 1300, 16), (50, 16, 1290, 12)]:
            base = start * 10000
            mix.update(start, base, start*100, signals, target, True)
            small = 80 if start == 50 else 50
            mix.update(start+10, base+received, start*100+small, signals, target, True)
            self.assertEqual(mix.target, expected, start)

    def test_noise_or_unsettled_slots_cannot_trigger_a_probe(self):
        from scheduler import AdaptiveMix
        mix = AdaptiveMix(64, 16, window=10)
        for t, active, retry in [(0, 16, 0), (9, 16, 1), (17, 16, 1),
                                  (18, 15, 1), (25, 16, 1), (34, 16, 1)]:
            mix.update(t, t*100, t, (retry, 0, None), active, True)
            self.assertEqual(mix.target, 16)
        mix.update(35, 3500, 35, (1, 0, None), 16, True)
        self.assertEqual(mix.target, 20)

    def test_equivalent_bandwidth_prefers_small_file_completions(self):
        from scheduler import AdaptiveMix
        mix = AdaptiveMix(64, 16, window=10)
        for t, net, small, actual in [(0, 0, 0, 16), (10, 1000, 20, 16),
                                      (11, 1000, 20, 20), (21, 2020, 30, 20)]:
            mix.update(t, net, small, (), actual, True)
        self.assertEqual(mix.target, 16)

    def test_requeued_files_keep_rank_at_both_ends(self):
        from scheduler import Pending
        pending = Pending([7, 5, 2, 4, 8])
        a = pending.take(True)
        b = pending.take(True)
        self.assertEqual((a[1], b[1]), (8, 4))
        self.assertEqual(pending.take(False)[1], 7)
        pending.put_back(b[0])
        pending.put_back(a[0])
        self.assertEqual([pending.take(False)[1], pending.take(True)[1],
                          pending.take(False)[1], pending.take(False)[1]], [5, 8, 2, 4])
        self.assertFalse(pending)

    def test_eta_includes_partial_and_inflight_bytes(self):
        from scheduler import RecentSpeed
        speed = RecentSpeed(0, 0, 0)
        raw, useful, eta = speed.read(10, 2000, 1000, remaining=6000)
        self.assertEqual((raw, useful, eta), (200, 100, 60))
        self.assertIsNone(speed.read(80, 2000, 1000, remaining=6000)[2])
