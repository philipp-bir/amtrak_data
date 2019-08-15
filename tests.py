import unittest
from amtrak_dataset import DatasetWriter
from contextlib import redirect_stdout
import io
import csv
from datetime import datetime
from pytz import timezone

class TestParsing(unittest.TestCase):

    def test_parse_comment(self):
        dw=DatasetWriter("/dev/null","/dev/null")
        self.assertEqual(dw._parse_comment("Departed  41 minutes late."), [("Departed",41)])
        self.assertEqual(dw._parse_comment("Arrived:  3 minutes late.            |  Departed:  On time."),[("Arrived",3),("Departed",0)])
        self.assertEqual(dw._parse_comment("Arrived:  6 minutes early."),[("Arrived",-6)])
        self.assertEqual(dw._parse_comment("Arrived:  2 hours, 15 minutes late.  |  Departed:  2 hours, 9 minutes late."),[("Arrived",2*60+15),("Departed",2*60+9)])
        self.assertEqual(dw._parse_comment("Arrived:  2 hours, 22 minutes late."),[("Arrived",2*60+22)])
        self.assertEqual(dw._parse_comment("Arrived  32 minutes late.  Estimated departure:  1 hour and 35 minutes late."),[("Arrived",32)])
        self.assertEqual(dw._parse_comment("Departed:  52 Minutes late."),[("Departed",52)])
        
    def test_missing_v_line(self):
        dw=DatasetWriter("/dev/null","/dev/null")
        test_input="""* Ethan Allen Express
* Trip shortened account CP track work.
* +---------------- Station code
* |    +----------- Schedule Arrival Day  
* |    |  +-------- Schedule Arrival time
* |    |  |     +----- Schedule Departure Day
* |    |  |     |  +-- Schedule Departure Time 
* |    |  |     |  |     +------------- Actual Arrival Time
* |    |  |     |  |     |     +------- Actual Departure Time
* |    |  |     |  |     |     |     +- Comments
* ALB  *  *     1  1100A *     1107A Departed:  7 minutes late.
* HUD  *  *     1  1125A *     1130A Departed:  5 minutes late.
* RHI  *  *     1  1146A *     1151A Departed:  5 minutes late.
* POU  *  *     1  1205P *     1208P Departed:  8 minutes late.
* CRT  *  *     1  1245P *     1251P Departed:  8 minutes late.
* YNY  *  *     1  104P  *     109P  Departed:  5 minutes late.
* NYP  1  135P  *  *     131P  *     Arrived:  4 minutes early."""
        f = io.StringIO()
        with open(dw.csv_station,"a") as csv_station:
            dw.csv_station_writer=csv.DictWriter(csv_station,
                dw.station_writer_fieldnames)
            with open(dw.csv_train,"a") as csv_train:
                dw.csv_train_writer=csv.DictWriter(csv_train,
                dw.train_writer_fieldnames)
                with redirect_stdout(f):
                    dw._handle_txt_file(io.BytesIO(test_input.encode()),"290_20100517.txt")
        
        s = f.getvalue()
        #print(s)
        self.assertIn("'V-line'",s)
        self.assertNotIn("Traceback",s)
        
    def handle_file(self,path,initial=True):
        dw=DatasetWriter("/dev/null","/dev/null")
        #csv_station = io.StringIO()
        with io.StringIO() as csv_station:
            dw.csv_station_writer=csv.DictWriter(csv_station,
                dw.station_writer_fieldnames)
            if initial:
                dw.csv_station_writer.writeheader()
            with io.StringIO() as csv_train:
                dw.csv_train_writer=csv.DictWriter(csv_train,
                dw.train_writer_fieldnames)
                if initial:
                    dw.csv_train_writer.writeheader()
                with open(path,"rb") as inner_txt:
                    dw._handle_txt_file(inner_txt,path)
                csv_station_text=csv_station.getvalue()
                csv_train_text=csv_train.getvalue()
        #self.assertTrue("SAV" in csv_train_text)
        return csv_train_text,csv_station_text
        
    def make_datetime(self,date_str,tz):
        return timezone(tz).localize(datetime.strptime(date_str, "%Y/%m/%d %I:%M%p"))
        
    def test_case_file1(self):
        path="test_cases/90_20090310.txt"
            
        #tc=["90_20090310.txt","90_20090416.txt","94_20100721.txt"]
        tt,st=self.handle_file(path)
        #print(tt)
        #print(st)
        self.assertIn("SAV",tt)
        self.assertIn("NYP",tt)
        self.assertIn("RVR",st)
        self.assertNotIn("Trai",tt)
        self.assertNotIn("Trai",st)
        
        self.assertIn("-27",tt)
        #print(str(datetime.strptime("2009/03/11 12:33AM", "%Y/%m/%d %I:%M%p")))
        arrival_date=self.make_datetime("2009/03/11 12:33AM","US/Eastern")
        #print(str(arrival_date))
        self.assertIn(str(arrival_date),tt)
        
        self.assertIn(str(self.make_datetime("2009/03/10 11:34AM","US/Eastern")),st)
     
    def test_case_file2(self):
        path="test_cases/94_20100721.txt"
            
        #tc=["90_20090310.txt","90_20090416.txt","94_20100721.txt"]
        tt,st=self.handle_file(path)
        #print(tt)
        #print(st)
        self.assertIn("NPN",tt)
        self.assertIn("BOS",tt)
        self.assertIn("NYP",st)
        self.assertNotIn("This",tt)
        self.assertNotIn("This",st)
        
        self.assertIn("13",tt) #delay

        #print(str(datetime.strptime("2009/03/11 12:33AM", "%Y/%m/%d %I:%M%p")))
        arrival_date=self.make_datetime("2010/07/21 10:12PM","US/Eastern")
        #print(str(arrival_date))
        self.assertIn(str(arrival_date),tt)
        
        self.assertIn(str(self.make_datetime("2010/07/21 02:06PM","US/Eastern")),st)

        
        #TODO one test with non-existant code
    def test_case_ne(self):
        path="test_cases/99xx_20120512.txt"
        tt,st=self.handle_file(path)
        #print(tt)
        #print(st)
        self.assertIn("XXX",tt)
        self.assertIn("XXX",st)
        self.assertIn("XXY",st)
        self.assertIn("YUM",st)
        self.assertIn("BOS",st)
        self.assertIn("XXZ",st)
        self.assertIn("XXW",st)
        self.assertIn("NYP",tt)
        self.assertIn("NYP",st)
        
        #times
        self.assertIn(str(self.make_datetime("2012/05/12 8:57AM","America/Phoenix")),tt)
        self.assertIn(str(self.make_datetime("2012/05/12 9:43AM","America/Phoenix")),st)
        self.assertIn(str(self.make_datetime("2012/05/12 11:29AM","America/Phoenix")),st)
        self.assertIn(str(self.make_datetime("2012/05/12 4:13PM","America/New_York")),st)
        self.assertIn(str(self.make_datetime("2012/05/13 12:50AM","America/New_York")),tt)
        
    def test_empty_file(self):
        path="test_cases/330_20130419.txt"
        tt,st=self.handle_file(path,initial=False)
        #print(tt)
        #print(st)
        self.assertEqual(tt,"")
        self.assertEqual(st,"")
        
    def test_cancel(self):
        path="test_cases/595_20111104.txt"
        tt,st=self.handle_file(path,initial=False)
        #print(tt)
        #print(st)
        #self.assertEqual(tt,"")
        #self.assertEqual(st,"")


if __name__ == '__main__':
    unittest.main()
