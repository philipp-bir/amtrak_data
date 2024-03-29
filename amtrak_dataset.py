from zipfile import ZipFile
import os.path
from io import BytesIO
import datetime
import csv
import re
import traceback
import pytz

class DatasetWriter:
    
    def __init__(self,csv_train,csv_station,csv_code_timezone="stations_timezone.csv"):
        self.csv_train=csv_train
        self.csv_train_writer=None
        self.csv_station=csv_station
        self.csv_station_writer=None
        self.current_fn=None
        self.success_train=0
        self.number_train=0
        self.success_station=0
        self.number_station=0
        
        self.station_writer_fieldnames=["train_id","station_code","station_nr","nr_of_stations",
                    "scheduled_arrival","actual_arrival","scheduled_departure",
                    "actual_departure","arrival_delay","departure_delay","delay"]
                    
        self.train_writer_fieldnames=["train_id","nr_of_stations","origin_station_code",
                    "scheduled_departure","actual_departure",
                    "destination_station_code","scheduled_arrival",
                    "actual_arrival","delay"]
        
        self.code_to_timezone={}
        timezones={
        "EST": pytz.timezone("America/New_York"),
        "CST": pytz.timezone("America/Chicago"),
        "MST": pytz.timezone("America/Denver"),
        "PST": pytz.timezone("America/Los_Angeles"),
        "MST/Arizona": pytz.timezone("America/Phoenix"),
        }
        with open(csv_code_timezone) as csv_code_tz:
            code_reader=csv.DictReader(csv_code_tz)
            for row in code_reader:
                tz=row["timezone"]
                if tz in timezones:
                    self.code_to_timezone[row["code"]]=timezones[tz]
        #print(self.code_to_timezone)

        
    def _parse_time(self,start_date,time_str,day_offset,timezone=None):
        try:
            time1=datetime.datetime.strptime((time_str+"M").rjust(6,"0"),"%I%M%p")
            diff=time1-datetime.datetime(1900,1,1)
            if timezone is not None:
                return timezone.localize(start_date+diff+datetime.timedelta(days=day_offset-1))
            return (start_date+diff+datetime.timedelta(days=day_offset-1))
        except ValueError as ve:
            #print(self.current_fn)
            #print(ve)
            return None
        
    def _delay_based_adj(self,delay_seconds):
        if delay_seconds<-4*3600:
            return datetime.timedelta(days=1)
        if delay_seconds>20*3600:
            return datetime.timedelta(days=-1)
        #if delay_seconds<-2*3600 or delay_seconds>18*3600:
        #    print(self.current_fn)
        #    print("Ambiguous delay time")
        return datetime.timedelta()
        
    def _parse_comment(self,comment):
        ret=[]
        while len(comment)>0:
            m=re.match(r"[^A-Za-z]*(Arrived|Departed):? ([^|]*?)([oO]n time|late|early)(.*)$",comment)
            if m is not None:
                if m.group(3)=="on time":
                    ret+=[(m.group(1),0)]
                else:
                    #print(m.group(2))
                    time_m=re.match(r"\D*((\d+) [hH]our)?\D*((\d+) [mM]in)?\D*$",m.group(2))
                    #print(time_m.groups())
                    sign=1 if m.group(3)=="late" else -1
                    if time_m is not None:
                        hour=int(time_m.group(2) or 0)
                        minute=int(time_m.group(4) or 0)
                        ret+=[(m.group(1),sign*(hour*60+minute))]
                    else:
                        print("Cannot parse '{}'".format(comment))
                comment=m.group(4)
            else:
                break
        return ret
    
    def _handle_txt_file(self,txt_file,file_name):

        try:
            self.number_train+=1
            #print(file_name)
            self.current_fn=file_name
            text=txt_file.read().decode()
            if len(text)==0:
                print("empty file")
                print(file_name)
                return
            if text[:len(text)//4].strip()==text[len(text)//2:][:len(text)//4].strip():
                #in rare occasions the file is containing an additional copy of the data
                text=text[:len(text)//2]
                print("double-file")
                print(file_name)
            text=text.strip()
            base_name,__=os.path.splitext(os.path.basename(file_name))
            train_id,date=base_name.split("_")
            year=int(date[:4])
            month=int(date[4:6])
            day=int(date[6:8])
            start_date=datetime.datetime(year=year,month=month,day=day)

            lines=text.split("\n")
            first=lines.pop(0)
            #if first in lines:
                
            #train_name=first.replace("*","").strip()  #this is unreliable
            
            train_data={"train_id":train_id}#,"train_name":train_name}
            station_data={"train_id":train_id,}
            #station_set=set()
            #print(train_data)
            indices=None
            start_idx=0
            ignore_lines=True
            repeat_idx=0
            #count_success=0
            #count_stations=0
            
            first_station_entry=True
            all_station_data=[]
            station_count=0
            last_valid_timezone=None
            
            for idx,line in enumerate(lines):
                if line.strip()=="CD":
                    continue
                if idx>=repeat_idx>0:
                    break
                if indices is None:
                    if repeat_idx==0 and len(line)>3 and line in lines[idx+1:]:
                        repeat_idx=lines[idx+1:].index(line)+idx+1
                        lines=lines[:repeat_idx]
                        print("repeating at line %d"%repeat_idx)
                        print(file_name)
                    if "+" in line:
                        ignore_lines=False
                        continue
                    if ignore_lines:
                        continue
                    indices=[]
                    last=0
                    
                    v_line_expected="V V    V  V     V  V     V     V     V"
                    v_line=line.replace("*","V").strip()
                    start_idx=idx+1
                    if v_line!=v_line_expected:
                        #unexpected 'V-line', we only accept it if it is just 'V' and ' '
                        m=re.match(r"^[ V]+$",v_line)
                        if m is None:
                            print("missing 'V-line', add one")
                            #print(line)
                            #print(v_line)
                            v_line=v_line_expected
                            start_idx=idx #this is so we start parsing this line as the first station
                    
                    #if (not "V" in line) or (len(set(line))>4):
                    #    print("No 'V'-line, add one")
                    #    v_line=
                    #    start_idx=idx
                    #else:    
                    #    start_idx=idx+1
                    #    v_line=line.replace("*","V")
                    #nr_stations=len(lines)-start_idx
                    while True:
                        last=v_line.find("V",last+1)
                        if last>=0:
                            indices+=[last]
                        else:
                            break
                    indices+=[-1]
                if indices is not None and idx>=start_idx:
                    
                    try:
                        last=0
                        collect=[]
                        for i in indices:
                            if i>=0:
                                collect+=[line[last:i].strip()]
                            else:
                                collect+=[line[last:].strip()]
                            last=i
                        #print(collect)
                        header={"station_code":1,"scheduled_arrival_day":2,"scheduled_arrival_time":3,
                            "scheduled_departure_day":4,"scheduled_departure_time":5,"actual_arrival_time":6,
                            "actual_departure_time":7,"comment":8}
                        scheduled_arrival=None
                        scheduled_departure=None
                        actual_arrival=None
                        actual_departure=None
                        arr_delay_min=None
                        dep_delay_min=None
                        
                        for i in [header["scheduled_arrival_day"],header["scheduled_departure_day"]]:
                            collect[i]=collect[i].replace("-","")
                        #print(sched_arr_str)
                        
                        station_code=collect[header["station_code"]]
                        #if station_code in station_set:
                        #    print("station already added.")
                        #    print(line)
                        #    print(file_name)
                        #    return
                            
                        if (len(station_code)!=3) or (re.match(r"[A-Z]{3}",station_code) is None):
                            #cannot identify a station code, means most likely a bad line (often a comment or an empty line)
                            #print(station_code)
                            continue
                            
                        if station_code in self.code_to_timezone:
                            tz=self.code_to_timezone[station_code]
                            if last_valid_timezone is None:
                                # this is the first time we have a timezone
                                # use it for all prior entries
                                # 
                                localize_or_none=lambda d:tz.localize(d) if d is not None else None
                                for s in all_station_data:
                                    for e in ["scheduled_arrival","actual_arrival","scheduled_departure","actual_departure"]:
                                        s[e]=localize_or_none(s[e])
                            last_valid_timezone=tz
                        self.number_station+=1

                        station_count+=1
                        
                        sched_dep_day=0
                        sched_arr_day=0
                        try:
                            sched_arr_day=int(collect[header["scheduled_arrival_day"]])
                        except ValueError:
                            pass
                        try:
                            sched_dep_day=int(collect[header["scheduled_departure_day"]])
                        except ValueError:
                            pass
                        
                        sched_arr_str=collect[header["scheduled_arrival_time"]]
                        if sched_arr_str!="*":
                            scheduled_arrival=self._parse_time(start_date,sched_arr_str,
                                sched_arr_day,last_valid_timezone)
                        sched_dep_str=collect[header["scheduled_departure_time"]]
                        if sched_dep_str!="*":
                            scheduled_departure=self._parse_time(start_date,sched_dep_str,
                                sched_dep_day,last_valid_timezone)
                        act_arr_str=collect[header["actual_arrival_time"]]
                        if len(act_arr_str)>1:
                            actual_arrival=self._parse_time(start_date,act_arr_str,sched_arr_day or sched_dep_day,last_valid_timezone)
                            if scheduled_arrival is not None and actual_arrival is not None:
                                #arr_delay=(actual_arrival-scheduled_arrival).total_seconds()
                                actual_arrival+=self._delay_based_adj((actual_arrival-scheduled_arrival).total_seconds())
                                arr_delay_min=int((actual_arrival-scheduled_arrival).total_seconds())//60
                                    
                        act_dep_str=collect[header["actual_departure_time"]]
                        #print(act_dep_str)
                        if len(act_dep_str)>1:
                            actual_departure=self._parse_time(start_date,act_dep_str,sched_dep_day or sched_arr_day,last_valid_timezone)
                            if scheduled_departure is not None and actual_departure is not None:
                                actual_departure+=self._delay_based_adj((actual_departure-scheduled_departure).total_seconds())
                                dep_delay_min=int((actual_departure-scheduled_departure).total_seconds())//60
                        
                        station_data["station_code"]=station_code
                        station_data["scheduled_arrival"]=scheduled_arrival
                        station_data["actual_arrival"]=actual_arrival
                        station_data["scheduled_departure"]=scheduled_departure
                        station_data["actual_departure"]=actual_departure
                        #station_data["nr_of_stations"]=nr_stations
                        station_data["station_nr"]=station_count
                        station_data["departure_delay"]=None
                        station_data["arrival_delay"]=None
                        
                        #print(station_data)
                        comment_data=self._parse_comment(collect[header["comment"]])
                        for aod,delay in comment_data:
                            if aod[0]=="D":
                                station_data["departure_delay"]=delay
                                d=dep_delay_min
                            elif aod[0]=="A":
                                station_data["arrival_delay"]=delay
                                d=arr_delay_min
                            if d is not None and abs(d-delay)==24*60:
                                sign=(d-delay)//abs(d-delay)
                                #one full day off, adjust accordingly
                                if aod[0]=="A" and station_data["actual_arrival"] is not None:
                                   station_data["actual_arrival"]+=datetime.timedelta(days=-sign)
                                   arr_delay_min=int((station_data["actual_arrival"]-scheduled_arrival).total_seconds())//60
                                if aod[0]=="D" and station_data["actual_departure"] is not None:
                                   station_data["actual_departure"]+=datetime.timedelta(days=-sign)
                                   dep_delay_min=int((station_data["actual_departure"]-scheduled_departure).total_seconds())//60
                        if station_data["arrival_delay"] is None:
                            station_data["arrival_delay"]=arr_delay_min
                        if station_data["departure_delay"] is None:
                            station_data["departure_delay"]=dep_delay_min
                        station_data["delay"]=station_data["arrival_delay"]
                        if station_data["delay"] is None:
                            station_data["delay"]=station_data["departure_delay"]

                        #if first_station_entry:
                        ##    train_data["origin_station_code"]=station_data["station_code"]
                        #    train_data["scheduled_departure"]=station_data["scheduled_departure"]
                        #    train_data["actual_departure"]=station_data["actual_departure"]
                        #    first_station_entry=False
                        #elif idx+1==len(lines):
                        #    train_data["destination_station_code"]=station_data["station_code"]
                        #    train_data["scheduled_arrival"]=station_data["scheduled_arrival"]
                        #    train_data["actual_arrival"]=station_data["actual_arrival"]
                        #    train_data["delay"]=station_data["delay"]
                        #self.csv_station_writer.writerow(station_data)
                        all_station_data+=[dict(station_data)]
                        self.success_station+=1
                    except ValueError as ve:
                        print(file_name)
                        print(line)
                        print(ve)
                        print(traceback.format_exc())
                        print(collect)
                    except KeyboardInterrupt as ki:
                        #print(file_name)
                        #print(self.delay_off)
                        raise ki
                    except:
                        print(file_name)
                        print(line)
                        print(traceback.format_exc())
                    #print(collect)
            
            #done with this train, write data
            
            
            #print(all_station_data)
            self.success_train+=1
            for station_data in all_station_data:
                station_data["nr_of_stations"]=station_count #now we know exactly how many stations
                self.csv_station_writer.writerow(station_data)
                
            #Gather the most relevant station information for the train entry
            first_station_data=all_station_data[0]
            last_station_data=all_station_data[-1]
            train_data["origin_station_code"]=first_station_data["station_code"]
            train_data["scheduled_departure"]=first_station_data["scheduled_departure"]
            train_data["actual_departure"]=first_station_data["actual_departure"]
            train_data["destination_station_code"]=last_station_data["station_code"]
            train_data["scheduled_arrival"]=last_station_data["scheduled_arrival"]
            train_data["actual_arrival"]=last_station_data["actual_arrival"]
            train_data["delay"]=last_station_data["delay"]
            train_data["nr_of_stations"]=station_count
            self.csv_train_writer.writerow(train_data)
            
        except ValueError as ve:
            print(file_name)
            print(line)
            print(ve)
            print(traceback.format_exc())
        except KeyboardInterrupt:
            print(file_name)
            #print(self.delay_off)
            exit()
        except:
            print(file_name)
            print(traceback.format_exc())
        #return (train_success,count_success/count_station)
        

    def _handle_zip(self,filename):
        #print(filename)
        with ZipFile(filename) as zip_file:
            #print(zipfile.namelist())
            for fn in zip_file.namelist():
                if os.path.basename(fn)!="":
                    __,ext=os.path.splitext(fn)
                    if ext==".zip":
                        zfiledata = BytesIO(zip_file.read(fn))
                        #with zip_file.open(f) as inner_zip:
                        self._handle_zip(zfiledata)
                    elif ext==".txt":
                        with zip_file.open(fn) as inner_txt:
                            self._handle_txt_file(inner_txt,fn)
                    elif ext==".log":
                        continue
                    else:
                        print("Cannot handle extension of '{}'!".format(fn))
                        
#    def 
                        
    def convert_zip(self,filename,initial=False):
        self.success_train=0
        self.number_train=0
        self.success_station=0
        self.number_station=0
        with open(self.csv_station,"w" if initial else "a") as csv_station:
            self.csv_station_writer=csv.DictWriter(csv_station, self.station_writer_fieldnames)
            if initial:
                self.csv_station_writer.writeheader()
            with open(self.csv_train,"w" if initial else "a") as csv_train:
                self.csv_train_writer=csv.DictWriter(csv_train, self.train_writer_fieldnames)
                if initial:
                    self.csv_train_writer.writeheader()
                self._handle_zip(filename)
        print("Converted '{}'. Success with {} out of {} trains ({}%) and {} out of {} stations ({}%)".format(
            filename,self.success_train,self.number_train,100*self.success_train/self.number_train,
            self.success_station,self.number_station,100*self.success_station/self.number_station))
        #print(self.delay_off)
       
if __name__ == '__main__':         
    dw=DatasetWriter("trains.csv","stations.csv")
    first=True
    for i in range(2007,2018):
        dw.convert_zip("/media/phil/DATA/trains/{}.zip".format(i),initial=first)
        first=False
