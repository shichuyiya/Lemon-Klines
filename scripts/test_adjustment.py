import tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import repair_adjustment as repair
import kline_snapshot as snapshot
import tdxlday

def bar(date,o=10,c=11,h=12,l=9,source='eastmoney-qfq'):
    return dict(date=date,open=o,close=c,high=h,low=l,volume=100,amount=1000,priceSource=source)

class AdjustmentTests(unittest.TestCase):
    def test_negative_adjusted_prices(self):
        self.assertTrue(repair.valid(bar('1993-04-07',-1,-.35,-.05,-1)))
    def test_anchor_scaling_is_disabled(self):
        with self.assertRaises(ValueError):tdxlday.apply_anchor_qfq([bar('d')],[bar('d')])
    def test_split_partial_session(self):
        raw=bar('2026-08-14',3,3.16,3.2,2.9,'eastmoney-raw')
        q=bar('2026-08-14',1,3.16/3,3.2/3,2.9/3)
        minute=bar('2026-08-14 15:00',3.1,3.16,3.18,3.08,'tencent-raw')
        fixed,n,unknown=repair.repair_minutes([minute],[],[q],[raw],'m30')
        self.assertEqual(unknown,[]);self.assertEqual(n,1);self.assertAlmostEqual(fixed[0]['close'],1.053333,places=6)
        self.assertEqual(fixed[0]['volume'],100)
    def test_unverifiable_session_is_reported(self):
        q=bar('2026-08-14');r=bar('2026-08-14',30,33,36,27)
        _,_,unknown=repair.repair_minutes([bar('2026-08-14 11:00',25,26,27,24)],[],[q],[r],'m30')
        self.assertEqual(unknown,['2026-08-14'])
    def test_auction_extreme_not_in_minute_is_allowed(self):
        daily=bar('2026-09-24',47.99,51.56,52.58,47.68)
        rows=[bar('2026-09-24 10:00',47.99,52.58,52.58,47.99),bar('2026-09-24 15:00',51.41,51.56,51.56,51.39)]
        fixed,n,unknown=repair.repair_minutes(rows,[],[daily],[daily],'m30')
        self.assertEqual(unknown,[]);self.assertEqual(n,0);self.assertEqual(len(fixed),2)
    def test_snapshot_refuses_mixed_sources(self):
        with patch.object(snapshot,'load_cache_bars',return_value=[bar('d1')]):
            with self.assertRaises(ValueError):snapshot.merge_bars('sh600641','m30',[bar('d2',source='tencent-raw')])
    def test_full_daily_replaces_old_prices(self):
        with patch.object(snapshot,'load_cache_bars',return_value=[bar('d1',30,31,32,29)]):
            self.assertEqual(snapshot.merge_bars('sh600641','day',[bar('d1')])[0]['close'],11)
    def test_daily_refresh_preserves_unverified_history_separately(self):
        old=[bar('d1'),bar('d2')]
        with patch.object(snapshot,'load_cache_bars',return_value=old),patch.object(snapshot,'load_cache_meta',return_value={'bars':old}):
            fresh=snapshot.merge_bars('sh600641','day',[bar('d2')])
            self.assertEqual(len(fresh),1)
            self.assertEqual(snapshot.QUARANTINED_HISTORY.pop(('sh600641','day'))['bars'],old)
    def test_expired_budget_stops_new_pagination(self):
        with patch.object(repair,'DEADLINE',1):
            with self.assertRaises(TimeoutError):repair.tencent_full('sh600641')
    def test_flat_limit_session_and_opening_auction(self):
        r=bar('2026-07-28',31.04,31.04,31.04,31.04)
        q=bar('2026-07-28',30,30,30,30)
        self.assertEqual(repair.transform(r,q,.025),(1.0,-1.0399999999999991))
        minute=bar('2026-07-28 09:30',30,30,30,30)
        self.assertTrue(repair.session_matches([minute],q,'m1',.0025))
if __name__=='__main__':unittest.main()
