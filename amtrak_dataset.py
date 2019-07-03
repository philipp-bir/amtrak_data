from zipfile import ZipFile
import os.path
from io import BytesIO
import datetime
import csv
import re
import traceback

class DatasetWriter:
    
    def __init__(self,csv_train,csv_station):
        self.csv_train=csv_train
        self.csv_train_writer=None
        self.csv_station=csv_station
        self.csv_station_writer=None
        self.current_fn=None
        
    def _parse_time(self,start_date,time_str,day_offset):
        try:
            time1=datetime.datetime.strptime((time_str+"M").rjust(6,"0"),"%I%M%p")
            diff=time1-datetime.datetime(1900,1,1)
            return start_date+diff+datetime.timedelta(days=day_offset-1)
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
            m=re.match(r"[^A-Za-z]*(Arrived|Departed):? ([^|]*)(on time|late|early)(.*)$",comment)
            if m is not None:
                if m.group(3)=="on time":
                    ret+=[(m.group(1),0)]
                else:
                    time_m=re.match(r"((\d+) hour)?\D*((\d+) min)?",m.group(2))
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
            #print(file_name)
            self.current_fn=file_name
            text=txt_file.read().decode()
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
                
            train_name=first.replace("*","").strip()  #this is unreliable
            
            train_data={"train_id":train_id}#,"train_name":train_name}
            station_data={"train_id":train_id,}
            station_set=set()
            #print(train_data)
            indices=None
            start_idx=0
            ignore_lines=True
            repeat_idx=0
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
                    start_idx=idx+1
                    nr_stations=len(lines)-start_idx
                    line=line.replace("*","V")
                    while True:
                        last=line.find("V",last+1)
                        if last>=0:
                            indices+=[last]
                        else:
                            break
                    indices+=[-1]
                else:
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
                        sched_arr_str=collect[header["scheduled_arrival_time"]]
                        for i in [header["scheduled_arrival_day"],header["scheduled_departure_day"]]:
                            collect[i]=collect[i].replace("-","")
                        #print(sched_arr_str)
                        if sched_arr_str!="*":
                            scheduled_arrival=self._parse_time(start_date,sched_arr_str,
                                int(collect[header["scheduled_arrival_day"]]))
                        sched_dep_str=collect[header["scheduled_departure_time"]]
                        if sched_dep_str!="*":
                            scheduled_departure=self._parse_time(start_date,sched_dep_str,
                                int(collect[header["scheduled_departure_day"]]))
                        act_arr_str=collect[header["actual_arrival_time"]]
                        if len(act_arr_str)>1:
                            actual_arrival=self._parse_time(start_date,act_arr_str,
                                int(collect[header["scheduled_arrival_day"]]))
                            if scheduled_arrival is not None and actual_arrival is not None:
                                #arr_delay=(actual_arrival-scheduled_arrival).total_seconds()
                                actual_arrival+=self._delay_based_adj((actual_arrival-scheduled_arrival).total_seconds())
                                arr_delay_min=int((actual_arrival-scheduled_arrival).total_seconds())//60
                                    
                        act_dep_str=collect[header["actual_departure_time"]]
                        #print(act_dep_str)
                        if len(act_dep_str)>1:
                            actual_departure=self._parse_time(start_date,act_dep_str,
                                int(collect[header["scheduled_departure_day"]]))
                            if scheduled_departure is not None and actual_departure is not None:
                                actual_departure+=self._delay_based_adj((actual_departure-scheduled_departure).total_seconds())
                                dep_delay_min=int((actual_departure-scheduled_departure).total_seconds())//60
                        station_code=collect[header["station_code"]]
                        if station_code in station_set:
                            print("station already added")
                            print(line)
                            print(file_name)
                            return
                        station_data["station_code"]=collect[header["station_code"]]
                        station_data["scheduled_arrival"]=scheduled_arrival
                        station_data["actual_arrival"]=actual_arrival
                        station_data["scheduled_departure"]=scheduled_departure
                        station_data["actual_departure"]=actual_departure
                        station_data["nr_of_stations"]=nr_stations
                        station_data["station_nr"]=idx-start_idx+1
                        
                        #print(station_data)
                        aod,delay=self._parse_comment(collect[header["comment"]])
                        if aod is not None:
                            if aod[0]=="D":
                                d=dep_delay_min
                            else:
                                d=arr_delay_min
                            if d is not None and abs(d-delay)==24*60:
                                sign=(d-delay)//abs(d-delay)
                                #one full day off, adjust accordingly
                                if station_data["actual_arrival"] is not None:
                                   station_data["actual_arrival"]+=datetime.timedelta(days=-sign)
                                   arr_delay_min=int((station_data["actual_arrival"]-scheduled_arrival).total_seconds())//60
                                if station_data["actual_departure"] is not None:
                                   station_data["actual_departure"]+=datetime.timedelta(days=-sign)
                                   dep_delay_min=int((station_data["actual_departure"]-scheduled_departure).total_seconds())//60
                                #print("adjusted")
                                ##print("before = {}, now= {}, {}.".format(d-delay,(arr_delay_min or 0)-delay,(dep_delay_min or 0)-delay))
                                #print(file_name)
                                #print(line)
                            station_data["delay"]=delay
                        elif arr_delay_min is not None:
                            station_data["delay"]=arr_delay_min
                        elif dep_delay_min is not None:
                            station_data["delay"]=dep_delay_min
                        else:
                            station_data["delay"]=None
                            #if d!=delay:
                            #    print(file_name)
                            #    print(line)
                            #    print("Delay doesn't agree ad={}, cd={}".format(d,delay))
                        if idx==start_idx:
                            train_data["origin_station_code"]=station_data["station_code"]
                            train_data["scheduled_departure"]=station_data["scheduled_departure"]
                            train_data["actual_departure"]=station_data["actual_departure"]
                        elif idx+1==len(lines):
                            train_data["destination_station_code"]=station_data["station_code"]
                            train_data["scheduled_arrival"]=station_data["scheduled_arrival"]
                            train_data["actual_arrival"]=station_data["actual_arrival"]
                            train_data["delay"]=station_data["delay"]
                        self.csv_station_writer.writerow(station_data)
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
            #print(train_data)
            self.csv_train_writer.writerow(train_data)
            #exit()
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
                        
    def convert_zip(self,filename):
        with open(self.csv_station,"a") as csv_station:
            self.csv_station_writer=csv.DictWriter(csv_station,
                fieldnames=["train_id","station_code","station_nr","nr_of_stations",
                    "scheduled_arrival","actual_arrival","scheduled_departure",
                    "actual_departure","delay"])
            with open(self.csv_train,"a") as csv_train:
                self.csv_train_writer=csv.DictWriter(csv_train,
                fieldnames=["train_id","train_name","origin_station_code",
                    "scheduled_departure","actual_departure",
                    "destination_station_code","scheduled_arrival",
                    "actual_arrival","delay"])
                self._handle_zip(filename)
        #print(self.delay_off)
                
dw=DatasetWriter("trains.csv","stations.csv")
for i in range(2006,2018):
    dw.convert_zip("/media/phil/DATA/trains/{}.zip".format(i))
