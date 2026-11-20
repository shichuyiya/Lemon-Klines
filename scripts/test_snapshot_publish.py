"""Run the real publish function with a fake GitHub boundary; no credentials or network."""
import ast,json,re,tempfile,time,unittest,urllib.parse
from datetime import datetime,timezone
from pathlib import Path
source=Path(__file__).with_name('kline_snapshot.py')
function=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='publish')

class PublishTests(unittest.TestCase):
    def run_case(self,fail=None):
        name='kline-snapshot-'+datetime.now(timezone.utc).strftime('%Y%m%d')+'.ksnap'
        calls=[]
        def gh(method,url,token,body=None,*args):
            calls.append((method,url,body))
            if method=='GET':return 200,json.dumps({'id':1,'assets':[{'id':9,'name':name}]}).encode()
            if method=='POST':
                if fail=='upload':return 503,b'failure'
                return 201,json.dumps({'id':10,'size':2 if fail=='size' else 1}).encode()
            if method=='PATCH' and url.endswith('/10') and fail in ('promotion','rollback'):return 503,b'failure'
            if method=='PATCH' and url.endswith('/9') and fail=='rollback' and json.loads(body)['name']==name:return 503,b'failure'
            return 200,b'{}'
        env=dict(json=json,re=re,time=time,urllib=urllib,datetime=datetime,timezone=timezone,Path=Path,gh=gh)
        exec(compile(ast.Module(body=[function],type_ignores=[]),str(source),'exec'),env)
        error=None
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'data.ksnap';p.write_bytes(b'x')
            try:env['publish'](p,'owner/repo','test',7)
            except RuntimeError as e:error=str(e)
        return calls,error,name
    def test_upload_failure_keeps_old_asset(self):
        calls,error,_=self.run_case('upload')
        self.assertIsNotNone(error);self.assertFalse(any(m in ('DELETE','PATCH') for m,_,_ in calls))
    def test_size_mismatch_keeps_old_asset(self):
        calls,error,_=self.run_case('size')
        self.assertIsNotNone(error);self.assertFalse(any(m in ('DELETE','PATCH') for m,_,_ in calls))
    def test_promotion_failure_rolls_back_without_deleting(self):
        calls,error,name=self.run_case('promotion')
        self.assertIsNotNone(error);self.assertFalse(any(m=='DELETE' for m,_,_ in calls))
        self.assertEqual(json.loads(calls[-1][2])['name'],name)
    def test_rollback_failure_reports_recoverable_original(self):
        calls,error,_=self.run_case('rollback')
        self.assertIn('original asset 9 retained',error);self.assertFalse(any(m=='DELETE' for m,_,_ in calls))
    def test_success_prunes_only_after_promotion(self):
        calls,error,_=self.run_case()
        self.assertIsNone(error)
        promote=next(i for i,(m,u,_) in enumerate(calls) if m=='PATCH' and u.endswith('/10'))
        delete=next(i for i,(m,_,_) in enumerate(calls) if m=='DELETE')
        self.assertGreater(delete,promote)
if __name__=='__main__':unittest.main()
