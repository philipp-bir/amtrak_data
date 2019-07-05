import unittest
from amtrak_dataset import DatasetWriter
from contextlib import redirect_stdout
import io
import csv

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
                fieldnames=["train_id","station_code","station_nr","nr_of_stations",
                    "scheduled_arrival","actual_arrival","scheduled_departure",
                    "actual_departure","arrival_delay","departure_delay","delay"])
            with open(dw.csv_train,"a") as csv_train:
                dw.csv_train_writer=csv.DictWriter(csv_train,
                fieldnames=["train_id","train_name","origin_station_code",
                    "scheduled_departure","actual_departure",
                    "destination_station_code","scheduled_arrival",
                    "actual_arrival","delay"])
                with redirect_stdout(f):
                    dw._handle_txt_file(io.BytesIO(test_input.encode()),"290_20100517.txt")
        
        s = f.getvalue()
        #print(s)
        self.assertTrue("'V'-line" in s)
        self.assertFalse("Traceback" in s)


if __name__ == '__main__':
    unittest.main()
